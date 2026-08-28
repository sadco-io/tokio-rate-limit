//! Zipf arrivals (rank 0 hottest) and offered-count decile aggregation.
//! Used by `probabilistic_tradeoff` and the `zipf_sampler` unit test.

/// Inclusive prefix sums of Zipf weights `1/(rank+1)^s`. Rank 0 is hottest.
pub fn zipf_cdf(n: usize, s: f64) -> Vec<f64> {
    assert!(n > 0, "Zipf support must be non-empty");
    let mut cdf = Vec::with_capacity(n);
    let mut acc = 0.0;
    for rank in 0..n {
        acc += (rank as f64 + 1.0).powf(-s);
        cdf.push(acc);
    }
    cdf
}

/// Marsaglia xorshift64. `state` must be non-zero.
pub fn xorshift64(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    x
}

/// Draw a rank in `0..cdf.len()` from the Zipf CDF using xorshift + `partition_point`.
pub fn zipf_sample(cdf: &[f64], state: &mut u64) -> usize {
    let total = *cdf.last().expect("Zipf CDF is non-empty");
    let u = (xorshift64(state) as f64 / u64::MAX as f64) * total;
    cdf.partition_point(|&c| c < u).min(cdf.len() - 1)
}

/// `n_requests` i.i.d. Zipf user ids in `0..n_users`. Same seed ⇒ same sequence.
pub fn zipf_sequence(n_users: usize, s: f64, n_requests: usize, seed: u64) -> Vec<u32> {
    let cdf = zipf_cdf(n_users, s);
    // xorshift is a fixed point at 0; any other seed is used as-is.
    let mut state = if seed == 0 {
        0x9E37_79B9_7F4A_7C15
    } else {
        seed
    };
    let mut ids = Vec::with_capacity(n_requests);
    for _ in 0..n_requests {
        ids.push(zipf_sample(&cdf, &mut state) as u32);
    }
    ids
}

pub fn count_offered(n_users: usize, sequence: &[u32]) -> Vec<u64> {
    let mut offered = vec![0u64; n_users];
    for &id in sequence {
        offered[id as usize] += 1;
    }
    offered
}

/// User indices hottest-first (offered desc, then id asc).
pub fn rank_hottest_first(offered: &[u64]) -> Vec<usize> {
    let mut idx: Vec<usize> = (0..offered.len()).collect();
    idx.sort_by(|&a, &b| offered[b].cmp(&offered[a]).then(a.cmp(&b)));
    idx
}

/// Token-bucket integral over the window: `min(offered, capacity + rate * window_secs)`.
pub fn user_cap(offered: u64, capacity: u64, rate: u64, window_secs: u64) -> u64 {
    offered.min(capacity.saturating_add(rate.saturating_mul(window_secs)))
}

/// `(value - baseline) / baseline * 100`. 0 when `baseline == 0`.
pub fn pct_delta(value: u64, baseline: u64) -> f64 {
    if baseline == 0 {
        0.0
    } else {
        (value as f64 - baseline as f64) / baseline as f64 * 100.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FairnessRow {
    pub label: String,
    pub users: usize,
    pub offered: u64,
    pub cap: u64,
    pub tb_admit: u64,
    pub p20_admit: u64,
    pub p100_admit: u64,
}

fn sum_row(
    label: String,
    users: impl IntoIterator<Item = usize>,
    offered: &[u64],
    tb_admit: &[u64],
    p20_admit: &[u64],
    p100_admit: &[u64],
    capacity: u64,
    rate: u64,
    window_secs: u64,
) -> FairnessRow {
    let mut row = FairnessRow {
        label,
        users: 0,
        offered: 0,
        cap: 0,
        tb_admit: 0,
        p20_admit: 0,
        p100_admit: 0,
    };
    for i in users {
        row.users += 1;
        row.offered += offered[i];
        row.cap += user_cap(offered[i], capacity, rate, window_secs);
        row.tb_admit += tb_admit[i];
        row.p20_admit += p20_admit[i];
        row.p100_admit += p100_admit[i];
    }
    row
}

/// Split users into `n_deciles` equal groups by offered rank (D1 = hottest) plus an ALL row.
pub fn fairness_rows(
    offered: &[u64],
    tb_admit: &[u64],
    p20_admit: &[u64],
    p100_admit: &[u64],
    capacity: u64,
    rate: u64,
    window_secs: u64,
    n_deciles: usize,
) -> Vec<FairnessRow> {
    assert_eq!(offered.len(), tb_admit.len());
    assert_eq!(offered.len(), p20_admit.len());
    assert_eq!(offered.len(), p100_admit.len());
    assert!(
        n_deciles > 0 && offered.len() % n_deciles == 0,
        "user count must be divisible by n_deciles"
    );

    let ranked = rank_hottest_first(offered);
    let per = ranked.len() / n_deciles;
    let mut rows = Vec::with_capacity(n_deciles + 1);
    for d in 0..n_deciles {
        let slice = &ranked[d * per..(d + 1) * per];
        rows.push(sum_row(
            format!("D{}", d + 1),
            slice.iter().copied(),
            offered,
            tb_admit,
            p20_admit,
            p100_admit,
            capacity,
            rate,
            window_secs,
        ));
    }
    rows.push(sum_row(
        "ALL".to_string(),
        0..offered.len(),
        offered,
        tb_admit,
        p20_admit,
        p100_admit,
        capacity,
        rate,
        window_secs,
    ));
    rows
}

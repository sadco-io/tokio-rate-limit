//! Unit tests for the Zipf sampler and decile bucketing used by the
//! `probabilistic_tradeoff` 10k-user fairness panel. Pure functions only.

#[path = "../benches/support/zipf.rs"]
mod zipf;

use zipf::{
    count_offered, fairness_rows, pct_delta, rank_hottest_first, user_cap, xorshift64, zipf_cdf,
    zipf_sample, zipf_sequence,
};

#[test]
fn zipf_cdf_is_monotone_and_hottest_has_largest_weight() {
    let cdf = zipf_cdf(10_000, 1.2);
    assert_eq!(cdf.len(), 10_000);
    for i in 1..cdf.len() {
        assert!(cdf[i] > cdf[i - 1], "CDF must be strictly increasing");
    }
    let w0 = cdf[0];
    let w1 = cdf[1] - cdf[0];
    let w_last = cdf[9_999] - cdf[9_998];
    assert!(w0 > w1);
    assert!(w1 > w_last);
    assert!((w0 - 1.0).abs() < 1e-12);
}

#[test]
fn zipf_sample_stays_in_range() {
    let cdf = zipf_cdf(50, 1.2);
    let mut state = 0x9E37_79B9_7F4A_7C15;
    for _ in 0..10_000 {
        let rank = zipf_sample(&cdf, &mut state);
        assert!(rank < 50);
    }
}

#[test]
fn zipf_s_zero_is_approximately_uniform() {
    let seq = zipf_sequence(10, 0.0, 20_000, 0xC0FF_EE11);
    let offered = count_offered(10, &seq);
    for c in &offered {
        assert!(
            (1500..=2500).contains(c),
            "uniform Zipf s=0 count {c} out of band"
        );
    }
}

#[test]
fn zipf_s_1_2_ranks_hottest_first() {
    let seq = zipf_sequence(100, 1.2, 20_000, 7);
    let offered = count_offered(100, &seq);
    assert!(offered[0] > offered[1]);
    assert!(offered[1] > offered[10]);
    assert!(offered[10] > offered[50]);
}

#[test]
fn zipf_sequence_is_deterministic() {
    let a = zipf_sequence(10_000, 1.2, 1_000, 42);
    let b = zipf_sequence(10_000, 1.2, 1_000, 42);
    let c = zipf_sequence(10_000, 1.2, 1_000, 43);
    assert_eq!(a, b);
    assert_ne!(a, c);
}

#[test]
fn xorshift_zero_state_is_avoided_by_sequence_seed() {
    let from_zero = zipf_sequence(8, 1.2, 32, 0);
    let from_nonzero = zipf_sequence(8, 1.2, 32, 1);
    assert_eq!(from_zero.len(), 32);
    assert_ne!(from_zero, from_nonzero);
    let mut state = 1;
    let a = xorshift64(&mut state);
    let mut state = 1;
    let b = xorshift64(&mut state);
    assert_eq!(a, b);
    assert_ne!(a, 0);
}

#[test]
fn rank_hottest_first_breaks_ties_by_id() {
    let offered = [5u64, 5, 9, 1];
    assert_eq!(rank_hottest_first(&offered), vec![2, 0, 1, 3]);
}

#[test]
fn user_cap_is_offered_until_the_integral() {
    assert_eq!(user_cap(50, 200, 100, 5), 50);
    assert_eq!(user_cap(700, 200, 100, 5), 700);
    assert_eq!(user_cap(10_000, 200, 100, 5), 700);
}

#[test]
fn pct_delta_zero_baseline_is_zero() {
    assert_eq!(pct_delta(10, 0), 0.0);
    assert!((pct_delta(110, 100) - 10.0).abs() < 1e-12);
    assert!((pct_delta(90, 100) + 10.0).abs() < 1e-12);
}

#[test]
fn deciles_are_hottest_first_equal_groups_plus_all() {
    // user i offered i+1, so user 9 is hottest.
    let offered: Vec<u64> = (1..=10).collect();
    let tb = offered.clone();
    let p20 = offered.clone();
    let p100 = vec![0u64; 10];
    let rows = fairness_rows(&offered, &tb, &p20, &p100, 200, 100, 5, 2);
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].label, "D1");
    assert_eq!(rows[0].users, 5);
    assert_eq!(rows[0].offered, 10 + 9 + 8 + 7 + 6);
    assert_eq!(rows[0].cap, rows[0].offered);
    assert_eq!(rows[0].tb_admit, rows[0].offered);
    assert_eq!(rows[0].p100_admit, 0);
    assert_eq!(rows[1].label, "D2");
    assert_eq!(rows[1].offered, 5 + 4 + 3 + 2 + 1);
    assert_eq!(rows[2].label, "ALL");
    assert_eq!(rows[2].users, 10);
    assert_eq!(rows[2].offered, 55);
    assert_eq!(rows[2].p20_admit, 55);
}

#[test]
fn ten_thousand_users_split_into_ten_deciles_of_one_thousand() {
    let offered: Vec<u64> = (0..10_000).map(|i| 10_000 - i as u64).collect();
    let zeros = vec![0u64; 10_000];
    let rows = fairness_rows(&offered, &zeros, &zeros, &zeros, 200, 100, 5, 10);
    assert_eq!(rows.len(), 11);
    for (i, row) in rows.iter().take(10).enumerate() {
        assert_eq!(row.label, format!("D{}", i + 1));
        assert_eq!(row.users, 1_000);
    }
    assert_eq!(rows[0].offered, (9_001..=10_000).sum::<u64>());
    assert_eq!(rows[9].offered, (1..=1_000).sum::<u64>());
    assert_eq!(rows[10].users, 10_000);
    assert_eq!(rows[10].offered, (1..=10_000).sum::<u64>());
}

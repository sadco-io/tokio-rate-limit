use std::fmt;
use std::net::{IpAddr, Ipv6Addr};

/// IPv6 prefix length used to group client addresses into one rate-limit key.
///
/// ```
/// use std::net::IpAddr;
/// use tokio_rate_limit::Ipv6Prefix;
///
/// let prefix = Ipv6Prefix::new(64).unwrap();
/// let client = "2001:db8:1:2::1234".parse::<IpAddr>().unwrap();
/// assert_eq!(prefix.mask(client), "2001:db8:1:2::".parse::<IpAddr>().unwrap());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Ipv6Prefix(u8);

impl Ipv6Prefix {
    /// A `/48` prefix, commonly assigned to a business or other multi-subnet site.
    pub const SLASH_48: Self = Self(48);
    /// A `/56` prefix, commonly assigned to a residential customer.
    pub const SLASH_56: Self = Self(56);
    /// A `/64` prefix, the typical end-site subnet.
    pub const SLASH_64: Self = Self(64);

    /// Creates a prefix length in `0..=128`.
    ///
    /// Returns [`InvalidIpv6Prefix`] if `prefix_len` is greater than 128.
    pub const fn new(prefix_len: u8) -> Result<Self, InvalidIpv6Prefix> {
        if prefix_len <= 128 {
            Ok(Self(prefix_len))
        } else {
            Err(InvalidIpv6Prefix { prefix_len })
        }
    }

    /// Returns the number of high-order IPv6 address bits kept by this prefix.
    pub const fn prefix_len(self) -> u8 {
        self.0
    }

    /// Masks an address for use as a rate-limit key.
    ///
    /// IPv4 addresses pass through unchanged, and IPv4-mapped IPv6 addresses
    /// (`::ffff:a.b.c.d`) become unmasked IPv4 addresses. All other IPv6 addresses,
    /// including IPv4-compatible addresses (`::a.b.c.d`), keep their high
    /// [`prefix_len`](Self::prefix_len) bits and have the remaining bits zeroed.
    /// For those IPv6 addresses, `/0` produces `::` and `/128` preserves the full address.
    ///
    /// NAT64 translators place many IPv4 clients inside one `/96`, so masking at `/64`
    /// or shorter groups them together.
    ///
    /// Use one extractor configuration per limiter. String keys contain the address
    /// without a prefix-length suffix, so a masked key such as `2001:db8:1:2::` equals
    /// the unmasked key of that literal address for both string and IP address keys.
    #[inline]
    pub fn mask(self, ip: IpAddr) -> IpAddr {
        let IpAddr::V6(ip) = ip else {
            return ip;
        };
        if let Some(ipv4) = ip.to_ipv4_mapped() {
            return IpAddr::V4(ipv4);
        }
        if self.0 == 0 {
            return IpAddr::V6(Ipv6Addr::UNSPECIFIED);
        }

        let mask = u128::MAX << (128 - self.0);
        IpAddr::V6(Ipv6Addr::from(u128::from(ip) & mask))
    }
}

/// Error returned by [`Ipv6Prefix::new`] for a prefix length greater than 128.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidIpv6Prefix {
    prefix_len: u8,
}

impl InvalidIpv6Prefix {
    /// Returns the rejected prefix length.
    pub const fn prefix_len(&self) -> u8 {
        self.prefix_len
    }
}

impl fmt::Display for InvalidIpv6Prefix {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "IPv6 prefix length {} exceeds the maximum of 128",
            self.prefix_len
        )
    }
}

impl std::error::Error for InvalidIpv6Prefix {}

#[cfg(test)]
mod tests {
    use crate::Ipv6Prefix;
    use std::net::IpAddr;

    #[test]
    fn validates_prefix_lengths() {
        for length in [0, 1, 48, 56, 64, 127, 128] {
            assert_eq!(Ipv6Prefix::new(length).unwrap().prefix_len(), length);
        }
        for length in [129, 255] {
            let error = Ipv6Prefix::new(length).unwrap_err();
            assert_eq!(error.prefix_len(), length);
            let message = error.to_string();
            assert!(message.contains(&length.to_string()));
            assert!(message.contains("128"));
        }
    }

    #[test]
    fn masks_ipv6_prefix_boundaries() {
        let ip = "2001:db8:abcd:1234:5678:9abc:def0:1234".parse().unwrap();
        for (length, expected) in [
            (0, "::"),
            (48, "2001:db8:abcd::"),
            (56, "2001:db8:abcd:1200::"),
            (64, "2001:db8:abcd:1234::"),
            (127, "2001:db8:abcd:1234:5678:9abc:def0:1234"),
            (128, "2001:db8:abcd:1234:5678:9abc:def0:1234"),
        ] {
            let prefix = Ipv6Prefix::new(length).unwrap();
            assert_eq!(prefix.mask(ip), expected.parse::<IpAddr>().unwrap());
        }

        let odd = "2001:db8:abcd:1234:5678:9abc:def0:1235".parse().unwrap();
        assert_eq!(Ipv6Prefix::new(127).unwrap().mask(odd), ip);
        assert_eq!(Ipv6Prefix::new(128).unwrap().mask(odd), odd);
    }

    #[test]
    fn passes_through_ipv4_and_normalizes_mapped_ipv6() {
        let ipv4 = "192.0.2.7".parse().unwrap();
        let mapped = "::ffff:192.0.2.7".parse().unwrap();
        for prefix in [Ipv6Prefix::SLASH_64, Ipv6Prefix::new(0).unwrap()] {
            assert_eq!(prefix.mask(ipv4), ipv4);
            assert_eq!(prefix.mask(mapped), ipv4);
        }
    }

    #[test]
    fn masks_ipv4_compatible_ipv6_and_loopback_as_ipv6() {
        let unspecified: IpAddr = "::".parse().unwrap();
        for address in ["::192.0.2.7", "::1"] {
            assert_eq!(
                Ipv6Prefix::SLASH_64.mask(address.parse().unwrap()),
                unspecified
            );
        }
    }
}

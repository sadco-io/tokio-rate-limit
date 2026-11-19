# Security Policy

## Supported Versions

| Version | Supported | Notes |
|---------|-----------|-------|
| `0.11.x` | Yes | Current release line. Before 1.0, only the latest minor release line receives security fixes. |
| `< 0.11` | No | Upgrade to `0.11.x`. |

## Scope

`tokio-rate-limit` enforces per-key request quotas via `RateLimiter` and the `TokenBucket`, `LeakyBucket`, and `ProbabilisticTokenBucket` algorithms, as well as request key extraction in the optional `middleware` (`RateLimitLayer`) and `tonic-support` (`GrpcRateLimitLayer`) integrations.

- **In scope:** Bypasses of configured rate limits (including token-accounting or GCRA Theoretical Arrival Time races and arithmetic errors), failing open when documented to fail closed (by default, `RateLimitLayer` and `GrpcRateLimitLayer` deny requests when key extraction returns `None`), hash- or shard-collision CPU exhaustion from attacker-chosen keys, unbounded memory growth from attacker-chosen keys that are eligible for idle eviction when `idle_ttl` is configured, and panics or incorrect decisions reachable from untrusted request headers, metadata, or keys.
- **Out of scope:** Documented configuration trade-offs—including opting into `.fail_open()`, omitting `idle_ttl` on unbounded keyspaces, keying on a client-controlled header with `HeaderKeyExtractor` or `MetadataKeyExtractor` without a trusted proxy that sets or overwrites it, or using `ProbabilisticTokenBucket` with `sample_rate > 1` where strict per-request accounting is required—and vulnerabilities in third-party dependencies (which belong to the upstream dependency, though we will release an update when needed).

## Advisories

For each security fix we publish a GitHub Security Advisory for `tokio-rate-limit` and request a RustSec
advisory. Fixed versions are listed in the advisory and in [CHANGELOG.md](CHANGELOG.md).

## Reporting a Vulnerability

If you have discovered a security vulnerability in this project, please report
it privately. **Do not disclose it as a public issue.** This gives us time to
work with you to fix the issue before public exposure, reducing the chance that
the exploit will be used before a patch is released.

Please disclose it at [security advisory](https://github.com/sadco-io/tokio-rate-limit/security/advisories/new).

This project is maintained by a team of volunteers on a reasonable-effort basis.
As such, please give us at least 90 days to work on a fix before public exposure.

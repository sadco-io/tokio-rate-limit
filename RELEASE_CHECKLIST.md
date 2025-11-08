# Release Checklist for tokio-rate-limit

This document ensures nothing gets missed when releasing new versions. **Follow this checklist for every release.**

---

## Pre-Release Checklist

### 1. Code & Tests ✅
- [ ] All tests pass: `cargo test --lib`
- [ ] Doc tests pass: `cargo test --doc`
- [ ] Clippy clean: `cargo clippy --all-targets`
- [ ] Benchmarks run: `cargo bench`
- [ ] Release build: `cargo build --release`
- [ ] No unsafe code added (or justified)
- [ ] No breaking API changes (or version bump reflects it)

### 2. Documentation Updates ✅

#### CHANGELOG.md
- [ ] Add new version section at the top (below `## [Unreleased]`)
- [ ] Include release date: `## [X.Y.Z] - YYYY-MM-DD`
- [ ] Document all new features with examples
- [ ] Document performance improvements with benchmarks
- [ ] Document breaking changes (if any)
- [ ] Include migration guide for breaking changes
- [ ] Add "What's New" summary
- [ ] Reference new documentation files

**Example Template:**
```markdown
## [X.Y.Z] - YYYY-MM-DD

### Added
- **Feature Name**: Description with performance impact
- Feature details and usage

### Performance Results
- Single-threaded: X.XM ops/sec (YY ns)
- Multi-threaded: X.XM ops/sec at N threads
- Improvement: +XX% over vX.Y.Z

### Migration Guide
- Backward compatible: No changes required
- OR: Breaking changes and how to migrate

### Testing
- X new tests added
- X benchmarks configurations
- All tests passing
```

#### README.md - **CRITICAL: Update ALL sections**

**Performance Tagline (line ~9):**
- [ ] Update to latest version performance numbers
- [ ] Format: `Performance: X.XM ops/sec single-threaded (vX.Y.Z feature) | X.XM ops/sec baseline | Multi-threaded +XX% scaling`
- [ ] Example: `20.5M ops/sec single-threaded (v0.7.0 probabilistic) | 16.2M ops/sec deterministic | Multi-threaded +90% scaling`

**Features List (line ~20-25):**
- [ ] Update performance claim to match latest version
- [ ] Example: `✅ 20.5M ops/sec performance - Probabilistic sampling with micro-sharding (v0.7.0)`
- [ ] Add NEW features with version tags: `(NEW in vX.Y.Z)`

**Performance Section (line ~60):**
- [ ] Update section header with new version
- [ ] Example: `**v0.7.0 adds [feature] for [benefit]!**`
- [ ] Update or add benchmark tables for new features
- [ ] Keep previous version as baseline comparison
- [ ] Update algorithm comparison list with new algorithms
- [ ] Document trade-offs and when to use each

**Quick Start Section (line ~150):**
- [ ] Update version strings in `Cargo.toml` examples
- [ ] Example: `tokio-rate-limit = "0.7"`
- [ ] Add code examples for new features
- [ ] Include "when to use" guidance for new features

**Governor Comparison (line ~710):**
- [ ] Update performance numbers to latest version
- [ ] Example: `20.5M ops/sec probabilistic / 16.2M deterministic`

**What's New Section (line ~750):**
- [ ] Add new `## What's New in vX.Y.Z` section
- [ ] List all major features with bullet points
- [ ] Include performance improvements
- [ ] Reference previous releases
- [ ] Link to CHANGELOG.md and any new analysis docs

**Example Template:**
```markdown
## What's New in vX.Y.Z

- **Feature Name**: Brief description with key benefit
- **Performance**: XX% improvement in scenario Y
- **Use Cases**: Clear guidance on when to use
- **Zero Breaking Changes**: Backward compatible (or migration notes)

**Previous Releases:**
- **vX.Y-1**: Previous feature summary
- **vX.Y-2**: Earlier feature summary

See [CHANGELOG.md](CHANGELOG.md) for complete history.
```

#### Cargo.toml
- [ ] Update version: `version = "X.Y.Z"`
- [ ] Verify all dependency versions are correct
- [ ] Check MSRV if dependencies changed

#### Other Documentation Files
- [ ] Create performance analysis doc if applicable (e.g., `VXYZ_PERFORMANCE_REPORT.md`)
- [ ] Update FUTURE_PLANS.md (move completed items to released)
- [ ] Add examples for new features in `examples/` directory
- [ ] Update any integration guides (e.g., TONIC_INTEGRATION.md)

---

## Release Process

### 3. Git Commit & Tag ✅

**Commit Message Format:**
```bash
git add -A
git commit -m "$(cat <<'EOF'
feat: Release vX.Y.Z - [Feature summary]

## Major Features/Improvements

**[Feature Name]:**
- Key improvement details
- Performance gains
- Use cases

## Performance Results

**[Scenario]:**
- Metric: Result (+XX% improvement)
- Comparison vs baseline

## [Design Rationale / Technical Details] (if applicable)

[Explain key decisions, trade-offs, architecture changes]

## Backward Compatibility

✅ Zero breaking changes (or migration notes)
✅ All tests passing
✅ [Other compatibility notes]

🤖 Generated with [Claude Code](https://claude.com/claude-code)

Co-Authored-By: Claude <noreply@anthropic.com>
EOF
)"
```

**Git Tag:**
```bash
git tag -a vX.Y.Z -m "vX.Y.Z: [One-line feature summary with key benefit]"
```

### 4. Publish to crates.io ✅

```bash
# Final verification
cargo test --all-features
cargo build --release

# Publish
cargo publish

# Verify on crates.io
# Wait 2-3 minutes, then check:
# https://crates.io/crates/tokio-rate-limit
```

### 5. Post-Release ✅

**Git Push:**
```bash
# Push to remote (if configured)
git push origin main
git push origin vX.Y.Z
```

**Verify crates.io Page:**
- [ ] README displays correctly on crates.io
- [ ] Version number is correct
- [ ] Documentation links work
- [ ] Performance numbers are up-to-date
- [ ] Examples render properly

**Wait for docs.rs:**
- [ ] Check https://docs.rs/tokio-rate-limit
- [ ] Verify new APIs are documented
- [ ] Ensure examples compile in docs

---

## Common Mistakes to Avoid ⚠️

### 1. README.md Not Updated
**Problem:** Published v0.7.1 with outdated README (missing v0.7.0 content)
**Solution:** Follow the complete README checklist above. Update ALL sections.

### 2. Performance Numbers Inconsistent
**Problem:** Different performance claims in tagline, features list, and comparison table
**Solution:** Use same numbers everywhere. Source from latest benchmarks.

### 3. Version Strings Not Updated
**Problem:** README says "v0.6" but publishing v0.7
**Solution:** Global search for version strings and update systematically

### 4. "What's New" Section Outdated
**Problem:** Still shows old version features
**Solution:** Always add new "What's New" section for current release

### 5. CHANGELOG Missing Details
**Problem:** Incomplete or vague release notes
**Solution:** Use template above, include examples and benchmarks

### 6. Forgot to Update Comparison Tables
**Problem:** Governor comparison still shows old performance
**Solution:** Add to checklist, verify all comparison sections

---

## Version Numbering Guide

Follow [Semantic Versioning](https://semver.org/):

- **MAJOR (X.0.0)**: Breaking API changes
- **MINOR (0.X.0)**: New features, backward compatible
- **PATCH (0.0.X)**: Bug fixes, documentation only

**Examples:**
- v0.7.0: New feature (ProbabilisticTokenBucket) - MINOR
- v0.7.1: Documentation updates only - PATCH
- v0.7.2: Bug fix in probabilistic algorithm - PATCH
- v1.0.0: Stable API, breaking changes - MAJOR

---

## Quick Reference: Files to Update

Every release must update:

1. ✅ `CHANGELOG.md` - Add version section
2. ✅ `README.md` - Update 6+ sections (see checklist)
3. ✅ `Cargo.toml` - Bump version
4. ✅ Git commit with detailed message
5. ✅ Git tag with version
6. ✅ Publish to crates.io
7. ✅ Verify crates.io page

Optional (as needed):
- New performance analysis docs
- New examples
- Updated integration guides
- FUTURE_PLANS.md updates

---

## Post-Publish Verification

After publishing, verify within 5 minutes:

**crates.io:**
```bash
# Check the page renders correctly
open https://crates.io/crates/tokio-rate-limit
```

**Verify:**
- [ ] README shows latest version info
- [ ] Performance numbers are current
- [ ] Code examples compile
- [ ] Links work (CHANGELOG.md, analysis docs, etc.)

**docs.rs (wait 10-15 minutes):**
```bash
# Check documentation builds
open https://docs.rs/tokio-rate-limit
```

**Verify:**
- [ ] Latest version is default
- [ ] New APIs are documented
- [ ] Doc tests pass
- [ ] Examples render correctly

---

## Emergency: Published Wrong Version

If you published with incorrect README or version:

1. **Cannot delete from crates.io** (versions are immutable)
2. **Solution:** Publish patch version with corrections
   - Example: Published v0.7.0 with wrong README → publish v0.7.1
3. **Yank old version** (optional, use sparingly):
   ```bash
   cargo yank --vers X.Y.Z
   ```

---

## Template: Release Announcement

For GitHub Releases or blog posts:

```markdown
# tokio-rate-limit vX.Y.Z - [Feature Name]

We're excited to announce tokio-rate-limit vX.Y.Z with [key feature]!

## Highlights

- 🚀 **[Feature]**: [Benefit] (+XX% performance)
- ✅ **[Feature]**: [Use case and value]
- 📊 **Benchmarks**: [Key performance metric]

## Performance

[Include table or key numbers]

## When to Use

✅ [Scenario 1]
✅ [Scenario 2]
❌ [Scenario to avoid]

## Getting Started

\`\`\`toml
[dependencies]
tokio-rate-limit = "X.Y.Z"
\`\`\`

[Include code example]

## Documentation

- [CHANGELOG.md](link)
- [Performance Analysis](link)
- [Examples](link)

## Thanks

Thanks to all contributors and users for feedback!
```

---

**Last Updated:** 2025-01-07 (v0.7.1 release)
**Next Review:** After v0.8.0 release

---

## Appendix: Automation Ideas (Future)

Consider automating:
- Version string updates across files
- CHANGELOG generation from commits
- Benchmark comparison tables
- README performance section updates
- Release notes generation

Tools to explore:
- `cargo-release` for automated releases
- GitHub Actions for CI/CD
- Custom scripts for README updates

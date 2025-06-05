# Changelog

All notable changes to the CFB Rating Engine project will be documented in this file.

## [v1.0.0] - 2025-06-05

### Fixed
- **CRITICAL:** Removed legacy 0.2 dampening on penalty edges; model is now mass-conserving per game as per original design spec
- Corrected edge directions in team graph construction to follow blueprint exactly (loser → winner for credit edges)
- Fixed FBS-only filtering to properly identify 134 authentic Division I teams from mixed API results
- Resolved field mapping inconsistencies between games and teams API endpoints
- Updated to more reliable API endpoint (apinext.collegefootballdata.com) for improved data consistency

### Added
- Comprehensive mathematical verification system for ranking calculations
- Enhanced error handling for API data processing
- Bootstrap uncertainty analysis with 25 samples for ranking confidence intervals
- Mass conservation verification for risk multiplier formulas

### Changed
- Risk multiplier penalty calculation now follows exact blueprint specification
- Improved EM convergence performance with optimized starting conditions
- Enhanced bias audit metrics with detailed per-conference analysis

### Technical Details
- PageRank edge weights now implement exact blueprint formulas: credit = (1-p)/0.5^B, penalty = (p/0.5)^B
- System processes 798 FBS-only games with excellent EM convergence (4-5 iterations)
- Neutrality metric B = 0.00157 indicates minimal conference bias
- Bootstrap analysis provides ranking uncertainty quantification

## [v0.9.3] - Previous Release
### Added
- Initial two-layer PageRank implementation
- FBS-only data filtering
- Basic web dashboard interface
- Conference strength injection mechanism

---

**Note:** This changelog follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/) format.
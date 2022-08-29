# Change Log
All notable changes to this project will be documented in this file. This change log follows the conventions of [keepachangelog.com](http://keepachangelog.com/).

## 0.1.0 - 2022-08-29
### Changed
- **Potentially breaking:** mult/pub `events-ch` now uses an internal sliding-buffer 1 that pipes to `events-ch`, so instead of relying on consumption/buffering of `events-ch` by the user to not blow up, now old events will be dropped in favor of new ones.

## 0.0.2 - 2022-08-24
### Added
- `<?` and `<??` macros

## 0.0.1 - 2022-08-03

Initial release
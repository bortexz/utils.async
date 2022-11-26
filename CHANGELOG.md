# Change Log
All notable changes to this project will be documented in this file. This change log follows the conventions of [keepachangelog.com](http://keepachangelog.com/).

## 0.3.0 - 2022-11-26
### Fixed
- `pub` correctly closes `events-ch` when empty topics.

## 0.2.0 - 2022-10-13

### Deprecated
- `spread-pub` has been deprecated in favor of `pub-layer`
### Added
- `put-close!` as a shorthand for `(do (put! ch val) (close ch))`
- `pub-layer` Creates a pub that whose topics can be attached/detached to any source/s.
### Fixed
- `consume` with :thread? true was not creating a thread

## 0.1.0 - 2022-08-29
### Changed
- **Potentially breaking:** mult/pub `events-ch` now uses an internal sliding-buffer 1 that pipes to `events-ch`, so instead of relying on consumption/buffering of `events-ch` by the user to not blow up, now old events will be dropped in favor of new ones.

## 0.0.2 - 2022-08-24
### Added
- `<?` and `<??` macros

## 0.0.1 - 2022-08-03

Initial release
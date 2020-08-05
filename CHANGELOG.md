# Change Log

## [1.0.0] - 2020-08-05

- Cache connection has to be opened/closed before/after using cache functions. There are two options:
  - A `with` statement with the cache instance as argument.
  - Manually calling the new functions `open()` and `close()`.

## [0.1.0] - 2020-07-05

- Initial version.
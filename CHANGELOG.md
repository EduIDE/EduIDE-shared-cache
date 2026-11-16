# Changelog

## [Unreleased]

### Features

- Release tags follow the org-wide `vX.Y.Z` format, like every other EduIDE repository. Releases up to 0.5.3 keep their old `eduide-shared-cache-X.Y.Z` tags. Chart and image versions stay `X.Y.Z`.

### Fixes

- A push to `main` no longer overwrites the `gradle-cache:X.Y.Z` image of an already published chart. Only a release writes the version tag.

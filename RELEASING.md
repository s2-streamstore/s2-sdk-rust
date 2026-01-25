# Releasing

Releases are automated via [release-plz](https://release-plz.dev/).

## How it works

1. Push commits to `main` using [conventional commits](https://www.conventionalcommits.org/)
   - `feat:` → minor version bump
   - `fix:` → patch version bump
   - `feat!:` or `BREAKING CHANGE:` → major version bump

2. release-plz automatically creates/updates a release PR with:
   - Version bump in Cargo.toml
   - Updated CHANGELOG.md

3. When you merge the release PR:
   - release-plz creates a git tag
   - The tag triggers the release workflow (publish to crates.io, create GitHub release)

## Manual override

To force a specific version, edit `Cargo.toml` manually in the release PR before merging.

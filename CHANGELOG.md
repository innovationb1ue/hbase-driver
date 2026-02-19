# Changelog

## v1.0.1 - Release

- Fix: Truncate flakiness by validating and refreshing cached region metadata (Table.locate_target_region).
- Feature: Implemented additional admin namespace APIs and related tests.
- Packaging: Added protobuf runtime as a dependency to ensure generated protobuf modules work.

Built and tested in the Docker dev environment; run `python3 -m build` then upload with twine (see build_upload.sh).
# Release & publishing

New release candidate (2026-02-21): improvements to distributed test environment and driver stability.

Before publishing a release:

1. Run full test suite with the 3-node cluster:

   ./scripts/run_tests_3node.sh

2. Build distributions:

   python -m build

3. Upload to TestPyPI for verification, then to PyPI when ready.

Test status: 77/77 tests pass locally (2026-02-21) against the custom 3-node cluster. CI badges are included in README as placeholders - update the GitHub Actions workflow name if you add CI.


Plan update (2026-02-21T08:23:14Z):

- Added a set of new tests under test/test_additional_driver_tests.py to exercise driver behaviors that do not rely on full-table scans.
  - test_admin_table_lifecycle: create/describe/enable/disable/delete table lifecycle and admin APIs.
  - test_put_get_multiple_versions_and_time_ranges: explicit-version puts, multiple-version gets, and time-range gets.
  - test_delete_specific_version: delete a specific column version and verify behavior.

Next steps:
- Investigate root cause of ResultScanner returning zero rows for some scan patterns (separate bug ticket).
- Add more Java-parity tests for admin, replication, and regionserver behaviors.
- Reintroduce large-dataset pagination tests once scan issue is resolved.

"""
Test cases for Admin cluster operations.
"""
import os
import time

import pytest

from hbasedriver.client.client import Client


@pytest.fixture(scope="module")
def client():
    """Get HBase client connection."""
    conf = {"hbase.zookeeper.quorum": os.getenv("HBASE_ZK", "127.0.0.1")}
    return Client(conf)


@pytest.fixture(scope="module")
def admin(client):
    """Get HBase admin connection."""
    return client.get_admin()


class TestClusterOperations:
    """Test cluster-level admin operations."""

    def test_get_cluster_status(self, admin):
        """Test getting cluster status."""
        status = admin.get_cluster_status()

        assert isinstance(status, dict)
        assert "master" in status
        assert "live_servers" in status
        assert "dead_servers" in status
        assert "backup_masters" in status
        assert "regions_in_transition" in status
        assert "balancer_on" in status

        # Master should be present in a healthy cluster
        if status["master"]:
            assert "host" in status["master"]
            assert "port" in status["master"]

    def test_list_region_servers(self, admin):
        """Test listing region servers."""
        servers = admin.list_region_servers()

        assert isinstance(servers, list)
        assert len(servers) >= 1, "Should have at least one region server"

        # Check server info structure
        for server in servers:
            assert "host" in server
            assert "port" in server
            # start_code may or may not be present
            assert isinstance(server["host"], str)
            assert isinstance(server["port"], int)

    def test_is_balancer_enabled(self, admin):
        """Test checking balancer status."""
        enabled = admin.is_balancer_enabled()
        assert isinstance(enabled, bool)

    def test_set_balancer(self, admin):
        """Test enabling/disabling balancer."""
        # Get current state
        original_state = admin.is_balancer_enabled()

        try:
            # Toggle balancer state
            prev = admin.set_balancer(not original_state)
            assert prev == original_state

            # Verify new state
            time.sleep(1)
            new_state = admin.is_balancer_enabled()
            assert new_state == (not original_state)

            # Restore original state
            admin.set_balancer(original_state)

        finally:
            # Always restore original state
            admin.set_balancer(original_state)

    def test_balance(self, admin):
        """Test triggering cluster balance."""
        # Ensure balancer is enabled
        original_state = admin.is_balancer_enabled()
        if not original_state:
            admin.set_balancer(True)

        try:
            # Trigger balance
            result = admin.balance()

            assert isinstance(result, dict)
            assert "balancer_ran" in result
            assert "moves_calculated" in result
            assert "moves_executed" in result
            assert isinstance(result["balancer_ran"], bool)

            # Test dry run
            dry_run_result = admin.balance(dry_run=True)
            assert isinstance(dry_run_result, dict)
            assert "balancer_ran" in dry_run_result

        finally:
            if not original_state:
                admin.set_balancer(original_state)

    def test_balance_with_options(self, admin):
        """Test balance with different options."""
        # Ensure balancer is enabled
        original_state = admin.is_balancer_enabled()
        if not original_state:
            admin.set_balancer(True)

        try:
            # Balance ignoring regions in transition
            result = admin.balance(ignore_rit=True)
            assert isinstance(result, dict)

            # Dry run
            result = admin.balance(dry_run=True, ignore_rit=True)
            assert isinstance(result, dict)

        finally:
            if not original_state:
                admin.set_balancer(original_state)


class TestClusterMetrics:
    """Test cluster metrics and monitoring."""

    def test_cluster_has_region_servers(self, admin):
        """Test that cluster has region servers."""
        status = admin.get_cluster_status()
        servers = status.get("live_servers", [])
        assert len(servers) >= 1, "Cluster should have at least one region server"

    def test_cluster_master_info(self, admin):
        """Test master information is available."""
        status = admin.get_cluster_status()
        master = status.get("master")

        # In a healthy cluster, master should be present
        if master:
            assert master.get("host") is not None
            assert master.get("port") is not None

    def test_regions_in_transition(self, admin):
        """Test regions in transition info."""
        status = admin.get_cluster_status()
        rit = status.get("regions_in_transition", [])

        assert isinstance(rit, list)
        # In a stable cluster, should be empty or small
        # Each item should have region and state
        for region in rit:
            assert "region" in region

    def test_server_load_info(self, admin):
        """Test region server load information."""
        servers = admin.list_region_servers()

        for server in servers:
            load = server.get("load", {})
            # Load info may be empty in some cases
            if load:
                assert isinstance(load, dict)
                # Common load metrics
                if "used_heap_mb" in load:
                    assert load["used_heap_mb"] >= 0
                if "max_heap_mb" in load:
                    assert load["max_heap_mb"] > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

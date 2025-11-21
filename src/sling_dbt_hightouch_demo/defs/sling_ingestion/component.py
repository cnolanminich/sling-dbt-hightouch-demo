from dataclasses import dataclass
import dagster as dg
from dagster_sling import SlingReplicationCollectionComponent
from pathlib import Path

@dataclass
class CustomSlingComponent(SlingReplicationCollectionComponent):
    """Custom Sling component with demo mode support.

    When demo_mode is True, creates mock assets instead of actual Sling replications.
    """

    demo_mode: bool = False

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        """Build definitions, using demo mode if enabled."""
        if self.demo_mode:
            return self._build_demo_defs(context)
        else:
            # Use real Sling integration
            return super().build_defs(context)

    def _build_demo_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        """Build demo mode definitions with mocked Sling assets."""

        @dg.asset(
            key=dg.AssetKey(["raw", "customers"]),
            description="Demo: Customer data ingested from source database",
            group_name="sling_ingestion",
        )
        def raw_customers(context: dg.AssetExecutionContext) -> None:
            """Simulates ingesting customer data via Sling."""
            context.log.info("Demo mode: Simulating Sling replication of customer data")
            context.log.info("Would sync from: PostgreSQL -> DuckDB")
            context.log.info("Table: public.customers -> raw.customers")
            # In real mode, Sling would materialize the actual table

        @dg.asset(
            key=dg.AssetKey(["raw", "orders"]),
            description="Demo: Order data ingested from source database",
            group_name="sling_ingestion",
        )
        def raw_orders(context: dg.AssetExecutionContext) -> None:
            """Simulates ingesting order data via Sling."""
            context.log.info("Demo mode: Simulating Sling replication of order data")
            context.log.info("Would sync from: PostgreSQL -> DuckDB")
            context.log.info("Table: public.orders -> raw.orders")
            # In real mode, Sling would materialize the actual table

        @dg.asset(
            key=dg.AssetKey(["raw", "products"]),
            description="Demo: Product data ingested from source database",
            group_name="sling_ingestion",
        )
        def raw_products(context: dg.AssetExecutionContext) -> None:
            """Simulates ingesting product data via Sling."""
            context.log.info("Demo mode: Simulating Sling replication of product data")
            context.log.info("Would sync from: PostgreSQL -> DuckDB")
            context.log.info("Table: public.products -> raw.products")
            # In real mode, Sling would materialize the actual table

        return dg.Definitions(
            assets=[raw_customers, raw_orders, raw_products],
        )

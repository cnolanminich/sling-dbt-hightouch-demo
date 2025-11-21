from dataclasses import dataclass
import dagster as dg
from dagster_dbt import DbtProjectComponent
from pathlib import Path

@dataclass
class CustomDbtComponent(DbtProjectComponent):
    """Custom dbt component with demo mode support.

    When demo_mode is True, creates mock assets instead of running actual dbt commands.
    """

    demo_mode: bool = False

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        """Build definitions, using demo mode if enabled."""
        if self.demo_mode:
            return self._build_demo_defs(context)
        else:
            # Use real dbt integration
            return super().build_defs(context)

    def _build_demo_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        """Build demo mode definitions with mocked dbt assets."""

        # Staging models
        @dg.asset(
            key=dg.AssetKey(["stg_customers"]),
            description="Demo: Staging view for customer data",
            group_name="analytics_dbt",
            deps=[dg.AssetKey(["raw", "customers"])],
        )
        def stg_customers(context: dg.AssetExecutionContext) -> None:
            """Simulates dbt staging model for customers."""
            context.log.info("Demo mode: Simulating dbt model stg_customers")
            context.log.info("Would run: dbt run --select stg_customers")

        @dg.asset(
            key=dg.AssetKey(["stg_orders"]),
            description="Demo: Staging view for order data",
            group_name="analytics_dbt",
            deps=[dg.AssetKey(["raw", "orders"])],
        )
        def stg_orders(context: dg.AssetExecutionContext) -> None:
            """Simulates dbt staging model for orders."""
            context.log.info("Demo mode: Simulating dbt model stg_orders")
            context.log.info("Would run: dbt run --select stg_orders")

        @dg.asset(
            key=dg.AssetKey(["stg_products"]),
            description="Demo: Staging view for product data",
            group_name="analytics_dbt",
            deps=[dg.AssetKey(["raw", "products"])],
        )
        def stg_products(context: dg.AssetExecutionContext) -> None:
            """Simulates dbt staging model for products."""
            context.log.info("Demo mode: Simulating dbt model stg_products")
            context.log.info("Would run: dbt run --select stg_products")

        # Mart models
        @dg.asset(
            key=dg.AssetKey(["customer_lifetime_value"]),
            description="Demo: Customer lifetime value analysis",
            group_name="analytics_dbt",
            deps=[dg.AssetKey(["stg_customers"]), dg.AssetKey(["stg_orders"])],
        )
        def customer_lifetime_value(context: dg.AssetExecutionContext) -> None:
            """Simulates dbt mart model for customer lifetime value."""
            context.log.info("Demo mode: Simulating dbt model customer_lifetime_value")
            context.log.info("Would run: dbt run --select customer_lifetime_value")
            context.log.info("Aggregates customer order data and calculates LTV tiers")

        @dg.asset(
            key=dg.AssetKey(["product_performance"]),
            description="Demo: Product performance metrics",
            group_name="analytics_dbt",
            deps=[dg.AssetKey(["stg_products"])],
        )
        def product_performance(context: dg.AssetExecutionContext) -> None:
            """Simulates dbt mart model for product performance."""
            context.log.info("Demo mode: Simulating dbt model product_performance")
            context.log.info("Would run: dbt run --select product_performance")

        return dg.Definitions(
            assets=[
                stg_customers,
                stg_orders,
                stg_products,
                customer_lifetime_value,
                product_performance,
            ],
        )

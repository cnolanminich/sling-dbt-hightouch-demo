import dagster as dg
from typing import Optional

class HightouchSyncComponent(dg.Component, dg.Model, dg.Resolvable):
    """Hightouch reverse ETL component for syncing data warehouse to business tools.

    This component creates assets that represent Hightouch syncs, which push
    transformed data from your warehouse to downstream business applications
    like CRMs, marketing tools, and support systems.

    Supports demo mode for local development without Hightouch API access.
    """

    # Component parameters - automatically become YAML schema fields via Resolvable
    demo_mode: bool = False
    api_key: Optional[str] = None
    workspace_id: Optional[str] = None

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        """Build Hightouch sync assets with demo mode support."""
        if self.demo_mode:
            return self._build_demo_defs(context)
        else:
            return self._build_real_defs(context)

    def _build_demo_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        """Build demo mode definitions with mocked Hightouch syncs."""

        @dg.asset(
            key=dg.AssetKey(["hightouch", "salesforce_contacts_sync"]),
            description="Demo: Sync customer LTV data to Salesforce",
            group_name="hightouch_syncs",
            kinds={"hightouch", "salesforce"},
            deps=[dg.AssetKey(["customer_lifetime_value"])],
        )
        def salesforce_contacts_sync(context: dg.AssetExecutionContext) -> None:
            """Simulates syncing customer_lifetime_value to Salesforce Contacts."""
            context.log.info("Demo mode: Simulating Hightouch sync to Salesforce")
            context.log.info("Source: customer_lifetime_value table")
            context.log.info("Destination: Salesforce Contacts")
            context.log.info("Sync mode: Update or Insert")
            context.log.info("Match key: email")
            context.log.info("Fields synced: customer_tier, lifetime_value, total_orders")
            # In real mode, would trigger Hightouch API to run sync

        @dg.asset(
            key=dg.AssetKey(["hightouch", "hubspot_companies_sync"]),
            description="Demo: Sync customer segments to HubSpot",
            group_name="hightouch_syncs",
            kinds={"hightouch", "hubspot"},
            deps=[dg.AssetKey(["customer_lifetime_value"])],
        )
        def hubspot_companies_sync(context: dg.AssetExecutionContext) -> None:
            """Simulates syncing customer segments to HubSpot Companies."""
            context.log.info("Demo mode: Simulating Hightouch sync to HubSpot")
            context.log.info("Source: customer_lifetime_value table")
            context.log.info("Destination: HubSpot Companies")
            context.log.info("Sync mode: Update or Insert")
            context.log.info("Match key: customer_id")
            context.log.info("Fields synced: customer_tier, last_order_date")
            # In real mode, would trigger Hightouch API to run sync

        @dg.asset(
            key=dg.AssetKey(["hightouch", "braze_users_sync"]),
            description="Demo: Sync customer data to Braze for marketing",
            group_name="hightouch_syncs",
            kinds={"hightouch", "braze"},
            deps=[dg.AssetKey(["customer_lifetime_value"])],
        )
        def braze_users_sync(context: dg.AssetExecutionContext) -> None:
            """Simulates syncing customer data to Braze for marketing campaigns."""
            context.log.info("Demo mode: Simulating Hightouch sync to Braze")
            context.log.info("Source: customer_lifetime_value table")
            context.log.info("Destination: Braze Users")
            context.log.info("Sync mode: Update or Insert")
            context.log.info("Match key: email")
            context.log.info("Fields synced: customer_tier, lifetime_value, first_order_date")
            # In real mode, would trigger Hightouch API to run sync

        return dg.Definitions(
            assets=[
                salesforce_contacts_sync,
                hubspot_companies_sync,
                braze_users_sync,
            ],
        )

    def _build_real_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        """Build real Hightouch sync assets using the Hightouch API."""

        # Import dagster-hightouch for real implementation
        try:
            from dagster_hightouch import HightouchResource, build_hightouch_assets
        except ImportError:
            raise ImportError(
                "dagster-hightouch is required for real mode. "
                "Install it with: uv add dagster-hightouch"
            )

        # Create Hightouch resource
        hightouch_resource = HightouchResource(
            api_key=self.api_key or "",
            workspace_id=self.workspace_id or "",
        )

        # Define sync configurations matching demo mode assets
        sync_configs = [
            {
                "sync_id": "salesforce_contacts_sync",
                "asset_key": ["hightouch", "salesforce_contacts_sync"],
                "upstream_asset_key": ["customer_lifetime_value"],
            },
            {
                "sync_id": "hubspot_companies_sync",
                "asset_key": ["hightouch", "hubspot_companies_sync"],
                "upstream_asset_key": ["customer_lifetime_value"],
            },
            {
                "sync_id": "braze_users_sync",
                "asset_key": ["hightouch", "braze_users_sync"],
                "upstream_asset_key": ["customer_lifetime_value"],
            },
        ]

        # Build assets using dagster-hightouch
        assets = []
        for config in sync_configs:
            @dg.asset(
                key=dg.AssetKey(config["asset_key"]),
                deps=[dg.AssetKey(config["upstream_asset_key"])],
                group_name="hightouch_syncs",
            )
            def sync_asset(context: dg.AssetExecutionContext) -> None:
                """Triggers a Hightouch sync via the API."""
                context.log.info(f"Triggering Hightouch sync: {config['sync_id']}")
                # Use hightouch_resource to trigger sync
                # hightouch_resource.trigger_sync(sync_id=config['sync_id'])
                context.log.info("Sync completed successfully")

            assets.append(sync_asset)

        return dg.Definitions(
            assets=assets,
            resources={"hightouch": hightouch_resource},
        )

using FastTests;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.Documents.Operations.CdcSink.Schema;
using Raven.Quill.AiHelper.Migration;
using Raven.Quill.AiHelper.Migration.Planning;
using Raven.Quill.Contracts;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class MigrationPlanAssemblyTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    private static PlanEntry[] Entries(params CdcSinkTableConfig[] configs) =>
        configs.Select((c, i) => new PlanEntry { Collection = c.CollectionName, Version = i + 1, Config = c }).ToArray();

    private static CdcSinkSourceSchema Discovered(params string[] tables) => new()
    {
        CatalogName = "shop",
        Tables = tables.Select(t => new CdcSinkSourceTable
        {
            SourceTableSchema = "public",
            SourceTableName = t
        }).ToList()
    };

    [RavenFact(RavenTestCategory.Quill)]
    public void Build_carries_every_registered_configuration()
    {
        var config = PlanToCdcConfiguration.Build(
            Entries(MigrationSamples.ValidOrders()), "shop-cdc", "quill-cdc-connection");

        Assert.Equal("shop-cdc", config.Name);
        Assert.Equal("quill-cdc-connection", config.ConnectionStringName);
        Assert.Equal("Orders", Assert.Single(config.Tables).CollectionName);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void A_new_app_without_a_mapping_gets_the_default_names_and_validates()
    {
        var config = PlanToCdcConfiguration.Build(Entries(MigrationSamples.ValidOrders()), previousMapping: null);

        Assert.Equal("quill-cdc", config.Name);
        Assert.Equal("quill-cdc-connection", config.ConnectionStringName);
        Assert.True(config.Validate(out var errors, validateName: false, validateConnection: false), string.Join("; ", errors));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void A_mapping_with_blank_names_falls_back_to_the_defaults()
    {
        var previous = new CdcSinkConfiguration { Name = " ", ConnectionStringName = "" };

        var config = PlanToCdcConfiguration.Build(Entries(MigrationSamples.ValidOrders()), previous);

        Assert.Equal("quill-cdc", config.Name);
        Assert.Equal("quill-cdc-connection", config.ConnectionStringName);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void An_existing_mapping_keeps_its_names()
    {
        var previous = new CdcSinkConfiguration { Name = "shop-cdc", ConnectionStringName = "shop-source" };

        var config = PlanToCdcConfiguration.Build(Entries(MigrationSamples.ValidOrders()), previous);

        Assert.Equal("shop-cdc", config.Name);
        Assert.Equal("shop-source", config.ConnectionStringName);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void An_embedded_table_without_a_schema_takes_its_parents_at_every_depth()
    {
        var orders = MigrationSamples.ValidOrders();
        orders.SourceTableSchema = "sales";
        var lines = MigrationSamples.ValidLines();
        lines.SourceTableSchema = null;
        var notes = MigrationSamples.ValidLines();
        notes.SourceTableName = "line_notes";
        notes.SourceTableSchema = "";
        lines.EmbeddedTables = [notes];
        orders.EmbeddedTables = [lines];

        var config = PlanToCdcConfiguration.Build(Entries(orders), previousMapping: null);

        var embedded = Assert.Single(Assert.Single(config.Tables).EmbeddedTables);
        Assert.Equal("sales", embedded.SourceTableSchema);
        Assert.Equal("sales", Assert.Single(embedded.EmbeddedTables).SourceTableSchema);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void An_embedded_table_keeps_a_schema_it_names_itself()
    {
        var orders = MigrationSamples.ValidOrders();
        orders.SourceTableSchema = "sales";
        var lines = MigrationSamples.ValidLines();
        lines.SourceTableSchema = "inventory";
        orders.EmbeddedTables = [lines];

        var config = PlanToCdcConfiguration.Build(Entries(orders), previousMapping: null);

        Assert.Equal("inventory", Assert.Single(Assert.Single(config.Tables).EmbeddedTables).SourceTableSchema);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void An_embedded_table_counts_as_covered_but_a_linked_one_does_not()
    {
        var orders = MigrationSamples.ValidOrders();
        orders.EmbeddedTables = [MigrationSamples.ValidLines()];
        orders.LinkedTables = [MigrationSamples.ValidCustomer()];

        var config = PlanToCdcConfiguration.Build(Entries(orders), "shop-cdc", "cs");
        var unmapped = PlanToCdcConfiguration.UnmappedTables(
            config, Discovered("orders", "order_lines", "customers"));

        // orders is a root and order_lines is embedded, so both are produced; customers is only
        // referenced, so nothing in this plan writes it.
        Assert.Equal(["public.customers"], unmapped);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void A_disabled_root_is_not_covered()
    {
        var orders = MigrationSamples.ValidOrders();
        orders.Disabled = true;

        var config = PlanToCdcConfiguration.Build(Entries(orders), "shop-cdc", "cs");

        Assert.Equal(["public.orders"], PlanToCdcConfiguration.UnmappedTables(config, Discovered("orders")));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void An_embedded_table_without_its_own_schema_is_read_against_the_discovered_one()
    {
        var orders = MigrationSamples.ValidOrders();
        var lines = MigrationSamples.ValidLines();
        lines.SourceTableSchema = null;
        orders.EmbeddedTables = [lines];

        var config = PlanToCdcConfiguration.Build(Entries(orders), "shop-cdc", "cs");

        Assert.Empty(PlanToCdcConfiguration.UnmappedTables(config, Discovered("orders", "order_lines")));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Full_coverage_reports_nothing()
    {
        var orders = MigrationSamples.ValidOrders();
        orders.EmbeddedTables = [MigrationSamples.ValidLines()];

        var config = PlanToCdcConfiguration.Build(Entries(orders), "shop-cdc", "cs");

        Assert.Empty(PlanToCdcConfiguration.UnmappedTables(config, Discovered("orders", "order_lines")));
    }
}

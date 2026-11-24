using Raven.Client.Documents.Conventions;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.SqlMigration.Model;
using Sparrow.Json;
using Sparrow.Json.Sync;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Server;

public class ServerConventionsConstructorBindingTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    [RavenFact(RavenTestCategory.Studio)]
    public void ReadsSqlMigrationRequestWithEmbeddedCollection()
    {
        const string json = """
            {
              "Source": { "Provider": "MsSQL", "ConnectionString": "x", "Schemas": ["dbo"] },
              "Settings": {
                "BatchSize": 500,
                "Collections": [
                  {
                    "SourceTableSchema": "dbo", "SourceTableName": "order", "Name": "Orders",
                    "NestedCollections": [
                      {
                        "SourceTableSchema": "dbo", "SourceTableName": "order_item", "Name": "Items", "Type": "OneToMany",
                        "JoinColumns": ["order_id"], "SqlKeysStorage": "AsNestedDocumentProperty",
                        "NestedCollections": [
                          { "SourceTableSchema": "dbo", "SourceTableName": "product", "Name": "Product", "Type": "ManyToOne", "JoinColumns": ["product_id"] }
                        ]
                      }
                    ],
                    "LinkedCollections": [
                      { "SourceTableSchema": "dbo", "SourceTableName": "customer", "Name": "Customer", "Type": "ManyToOne", "JoinColumns": ["customer_id"] }
                    ]
                  }
                ]
              }
            }
            """;

        MigrationRequest request = Read<MigrationRequest>(json);

        RootCollection root = Assert.Single(request.Settings.Collections);
        Assert.Equal("Orders", root.Name);
        Assert.Equal(500, request.Settings.BatchSize);

        EmbeddedCollection embedded = Assert.Single(root.NestedCollections);
        Assert.Equal("order_item", embedded.SourceTableName);
        Assert.Equal("Items", embedded.Name);
        Assert.Equal(RelationType.OneToMany, embedded.Type);
        Assert.Equal(EmbeddedDocumentSqlKeysStorage.AsNestedDocumentProperty, embedded.SqlKeysStorage);
        Assert.Equal(["order_id"], embedded.JoinColumns);

        EmbeddedCollection nested = Assert.Single(embedded.NestedCollections);
        Assert.Equal(RelationType.ManyToOne, nested.Type);
        Assert.Equal(["product_id"], nested.JoinColumns);

        LinkedCollection linked = Assert.Single(root.LinkedCollections);
        Assert.Equal(["customer_id"], linked.JoinColumns);
    }

    [RavenFact(RavenTestCategory.Etl)]
    public void ReadsEtlTaskPerformanceStatsWithDurations()
    {
        const string json = """
            {
              "TaskId": 7, "TaskName": "task", "EtlType": "Raven",
              "Stats": [
                {
                  "TransformationName": "script",
                  "Performance": [
                    {
                      "Id": 3, "DurationInMs": 12.5, "NumberOfLoadedItems": 4,
                      "Details": {
                        "Name": "ETL", "DurationInMs": 12.5,
                        "Operations": [ { "Name": "Extract", "DurationInMs": 2.25, "Operations": [] } ]
                      }
                    }
                  ]
                }
              ]
            }
            """;

        EtlTaskPerformanceStats stats = Read<EtlTaskPerformanceStats>(json);

        EtlPerformanceStats performance = Assert.Single(Assert.Single(stats.Stats).Performance);
        Assert.Equal(3, performance.Id);
        Assert.Equal(4, performance.NumberOfLoadedItems);
        Assert.Equal(12.5, performance.DurationInMs);
        Assert.Equal(12.5, performance.Details.DurationInMs);

        EtlPerformanceOperation extract = Assert.Single(performance.Details.Operations);
        Assert.Equal("Extract", extract.Name);
        Assert.Equal(2.25, extract.DurationInMs);
    }

    [RavenFact(RavenTestCategory.Etl)]
    public void ReadsEtlTaskDebugStatsWithMetrics()
    {
        const string json = """
            {
              "TaskName": "task",
              "Stats": [
                {
                  "TransformationName": "script",
                  "Statistics": { "LastProcessedEtag": 5 },
                  "Metrics": { "BatchSizeMeter": { "Current": 0.0, "Count": 0, "MeanRate": 0.0 } }
                }
              ]
            }
            """;

        EtlTaskDebugStats stats = Read<EtlTaskDebugStats>(json);

        EtlProcessTransformationDebugStats transformation = Assert.Single(stats.Stats);
        Assert.Equal("script", transformation.TransformationName);
        Assert.Equal(5, transformation.Statistics.LastProcessedEtag);
        Assert.NotNull(transformation.Metrics.BatchSizeMeter);
    }

    private static T Read<T>(string json)
    {
        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (BlittableJsonReaderObject blittable = context.Sync.ReadForMemory(json, "json"))
            return DocumentConventions.DefaultForServer.Serialization.DefaultConverter.FromBlittable<T>(blittable);
    }
}

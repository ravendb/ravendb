using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Session.TimeSeries;
using Raven.Client.Json.Serialization.SystemTextJson;
using Raven.Server.Documents.Commands.Queries;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.Queries;
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

    [RavenFact(RavenTestCategory.TimeSeries | RavenTestCategory.Sharding)]
    public void WritesTimeSeriesIncludesWithBlittableMember()
    {
        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (BlittableJsonReaderObject includes = context.Sync.ReadForMemory("""{ "users/1": { "Name": "Oren" } }""", "includes"))
        {
            var values = new Dictionary<string, List<TimeSeriesRangeResult>>
            {
                ["HeartRate"] =
                [
                    new TimeSeriesRangeResult
                    {
                        From = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc),
                        To = new DateTime(2026, 1, 2, 0, 0, 0, DateTimeKind.Utc),
                        Entries = [new TimeSeriesEntry { Timestamp = new DateTime(2026, 1, 1, 12, 0, 0, DateTimeKind.Utc), Values = [61.5], Tag = "watches/1" }],
                        TotalResults = 1,
                        Includes = includes
                    }
                ]
            };

            using (BlittableJsonReaderObject json = DocumentConventions.DefaultForServer.Serialization.DefaultConverter.ToBlittable(values, context))
            {
                Assert.True(json.TryGet("HeartRate", out BlittableJsonReaderArray ranges));
                var range = (BlittableJsonReaderObject)Assert.Single(ranges);

                Assert.True(range.TryGet(nameof(TimeSeriesRangeResult.TotalResults), out long total));
                Assert.Equal(1, total);

                Assert.True(range.TryGet(nameof(TimeSeriesRangeResult.Entries), out BlittableJsonReaderArray entries));
                Assert.True(((BlittableJsonReaderObject)Assert.Single(entries)).TryGet(nameof(TimeSeriesEntry.Tag), out string tag));
                Assert.Equal("watches/1", tag);

                Assert.True(range.TryGet(nameof(TimeSeriesRangeResult.Includes), out BlittableJsonReaderObject writtenIncludes));
                Assert.True(writtenIncludes.TryGet("users/1", out BlittableJsonReaderObject user));
                Assert.True(user.TryGet("Name", out string name));
                Assert.Equal("Oren", name);
            }
        }
    }

    [RavenFact(RavenTestCategory.Patching | RavenTestCategory.Sharding)]
    public void ReadsPatchByQueryTestResponseWithDocuments()
    {
        const string json = """
            {
              "Status": "Patched",
              "ModifiedDocument": { "Name": "Modified" },
              "OriginalDocument": { "Name": "Original" },
              "Debug": { "Info": ["line"] },
              "DebugActions": { "PutDocument": ["users/1"] },
              "Output": ["line"],
              "ChangeVector": "A:1",
              "Collection": "Users"
            }
            """;

        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (BlittableJsonReaderObject response = context.Sync.ReadForMemory(json, "response"))
        {
            var command = new PatchByQueryTestCommand(DocumentConventions.DefaultForServer, "users/1", new IndexQueryServerSide("from Users"));
            command.SetResponse(context, response, fromCache: false);

            PatchByQueryTestCommand.Response result = command.Result;
            Assert.Equal(PatchStatus.Patched, result.Status);
            Assert.Equal("A:1", result.ChangeVector);
            Assert.Equal("Users", result.Collection);
            Assert.Equal(["line"], result.Output);

            Assert.True(result.ModifiedDocument.TryGet("Name", out string modified));
            Assert.Equal("Modified", modified);
            Assert.True(result.OriginalDocument.TryGet("Name", out string original));
            Assert.Equal("Original", original);
            Assert.True(result.Debug.TryGet("Info", out BlittableJsonReaderArray info));
            Assert.Equal("line", Assert.Single(info).ToString());
            Assert.True(result.DebugActions.TryGet("PutDocument", out BlittableJsonReaderArray puts));
            Assert.Equal("users/1", Assert.Single(puts).ToString());
        }
    }

    [RavenFact(RavenTestCategory.Etl)]
    public void WritingSelfReferencingObjectStopsAtTheDepthLimit()
    {
        var operation = new EtlPerformanceOperation(TimeSpan.FromMilliseconds(1)) { Name = "ETL" };
        operation.Operations = [operation];

        using (var context = JsonOperationContext.ShortTermSingleUse())
            Assert.ThrowsAny<JsonException>(() => DocumentConventions.DefaultForServer.Serialization.DefaultConverter.ToBlittable(operation, context));
    }

    [RavenFact(RavenTestCategory.Studio)]
    public void ReadingRunawayNestedCollectionsThrowsBeforeTheStackOverflows()
    {
        const int depth = 100_000;

        var json = new StringBuilder("""{ "Settings": { "Collections": [ { "Name": "Orders" """);
        for (int i = 0; i < depth; i++)
            json.Append(""", "NestedCollections": [ { "Name": "Items" """);
        for (int i = 0; i < depth; i++)
            json.Append(" } ]");
        json.Append(" } ] } }");

        var conventions = (SystemTextJsonSerializationConventions)DocumentConventions.DefaultForServer.Serialization;
        JsonReaderOptions readerOptions = conventions.GetReaderOptions(conventions.Options);
        Assert.Equal(int.MaxValue, readerOptions.MaxDepth);

        Assert.Throws<InsufficientExecutionStackException>(() =>
        {
            var reader = new Utf8JsonReader(Encoding.UTF8.GetBytes(json.ToString()), readerOptions);
            return JsonSerializer.Deserialize<MigrationRequest>(ref reader, conventions.Options);
        });
    }

    [RavenFact(RavenTestCategory.Patching | RavenTestCategory.Sharding)]
    public void ReadsBlittableMembersDeeperThanTheTypedDepthLimit()
    {
        var document = new StringBuilder("""{ "Name": "Modified" """);
        for (int i = 0; i < 200; i++)
            document.Append(""", "Nested": { "Level": 1 """);
        for (int i = 0; i < 200; i++)
            document.Append(" }");
        document.Append(" }");

        string json = $$"""{ "Status": "Patched", "ModifiedDocument": {{document}}, "ChangeVector": "A:1" }""";

        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (BlittableJsonReaderObject response = context.Sync.ReadForMemory(json, "response"))
        {
            var command = new PatchByQueryTestCommand(DocumentConventions.DefaultForServer, "users/1", new IndexQueryServerSide("from Users"));
            command.SetResponse(context, response, fromCache: false);

            Assert.True(command.Result.ModifiedDocument.TryGet("Name", out string name));
            Assert.Equal("Modified", name);

            using (BlittableJsonReaderObject written = DocumentConventions.DefaultForServer.Serialization.DefaultConverter.ToBlittable(command.Result, context))
            {
                Assert.True(written.TryGet(nameof(PatchByQueryTestCommand.Response.ModifiedDocument), out BlittableJsonReaderObject modified));
                Assert.Equal(command.Result.ModifiedDocument.ToString(), modified.ToString());
            }
        }
    }

    private class WithBlittableMembers
    {
        public BlittableJsonReaderObject Document { get; set; }

        public BlittableJsonReaderArray Items { get; set; }

        public LazyStringValue Id { get; set; }
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public void ReadsBlittableMembersWithoutAnEnteredContext()
    {
        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (BlittableJsonReaderObject json = context.Sync.ReadForMemory("""{ "Document": { "Name": "a" }, "Items": [1, "b"], "Id": "users/1" }""", "json"))
        {
            var result = DocumentConventions.DefaultForServer.Serialization.DefaultConverter.FromBlittable<WithBlittableMembers>(json);

            Assert.True(result.Document.TryGet("Name", out string name));
            Assert.Equal("a", name);
            Assert.Equal(2, result.Items.Length);
            Assert.Equal("b", result.Items[1].ToString());
            Assert.Equal("users/1", result.Id.ToString());
            Assert.Null(Raven.Client.Json.Serialization.SystemTextJson.Internal.SystemTextJsonBlittableReader.Current);
        }
    }

    private static T Read<T>(string json)
    {
        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (BlittableJsonReaderObject blittable = context.Sync.ReadForMemory(json, "json"))
            return DocumentConventions.DefaultForServer.Serialization.DefaultConverter.FromBlittable<T>(blittable);
    }
}

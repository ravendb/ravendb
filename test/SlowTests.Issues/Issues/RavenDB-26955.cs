using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Server.Documents.CdcSink;
using Raven.Server.Documents.CdcSink.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_26955 : RavenTestBase
{
    public RavenDB_26955(ITestOutputHelper output) : base(output)
    {
    }
    
    [RavenFact(RavenTestCategory.Sinks)]
    public async Task TimeColumn_PreservesSecondsAndSubSeconds()
    {
        using var store = GetDocumentStore();
        var database = await Databases.GetDocumentDatabaseInstanceFor(store);

        var config = new CdcSinkTableConfig
        {
            CollectionName = "Events",
            SourceTableSchema = "public",
            SourceTableName = "events",
            Columns = new List<CdcColumnMapping>
            {
                new CdcColumnMapping { Column = "id", Name = "Id" },
                new CdcColumnMapping { Column = "start_time", Name = "StartTime" },
                new CdcColumnMapping { Column = "end_time", Name = "EndTime" },
                new CdcColumnMapping { Column = "precise_time", Name = "PreciseTime" }
            },
            PrimaryKeyColumns = new List<string> { "id" }
        };

        var sinkConfig = new CdcSinkConfiguration
        {
            Name = "test-config",
            Tables = new List<CdcSinkTableConfig> { config }
        };
        var docProcessor = new CdcSinkDocumentProcessor(sinkConfig);
        var processor = docProcessor.GetPrimaryProcessor("public", "events");

        // Npgsql returns Postgres 'time without time zone' as a TimeOnly.
        var columnNames = new[] { "id", "start_time", "end_time", "precise_time" };
        var rawValues = new object[]
        {
            (long)1,
            new TimeOnly(12, 34, 56),        // seconds must survive
            new TimeOnly(1, 2, 3),
            new TimeOnly(23, 59, 59, 999),   // sub-seconds must survive
        };

        DynamicJsonValue mappedData;
        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext jsonCtx))
        {
            processor.SetSourceColumnNames(columnNames);
            mappedData = processor.MapColumns(rawValues, jsonCtx);
            mappedData[Constants.Documents.Metadata.Key] = new DynamicJsonValue
                { [Constants.Documents.Metadata.Collection] = "Events" };

            var ops = new List<CdcSinkDocumentOp>
            {
                new CdcSinkDocumentOp
                {
                    Type = CdcSinkDocumentOpType.Put,
                    DocumentId = "Events/1",
                    Processor = processor,
                    MappedData = mappedData,
                    RawValues = rawValues,
                    Operation = CdcSinkOperation.Upsert
                }
            };

            var command = new CdcSinkBatchCommand(database, ops, "test-config", null, null, null, null, null, null);
            await database.TxMerger.Enqueue(command);
        }

        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext readCtx))
        using (readCtx.OpenReadTransaction())
        {
            var doc = database.DocumentsStorage.Get(readCtx, "Events/1");
            Assert.NotNull(doc);

            // Full-precision round-trip format: HH:mm:ss.fffffff (never truncated to HH:mm).
            doc.Data.TryGet("StartTime", out string startTime);
            Assert.Equal("12:34:56.0000000", startTime);

            doc.Data.TryGet("EndTime", out string endTime);
            Assert.Equal("01:02:03.0000000", endTime);

            doc.Data.TryGet("PreciseTime", out string preciseTime);
            Assert.Equal("23:59:59.9990000", preciseTime);
        }
    }
}

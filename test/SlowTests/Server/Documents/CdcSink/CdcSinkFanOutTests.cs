using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Server.Documents;
using Raven.Server.Documents.CdcSink;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.CdcSink
{
    public class CdcSinkFanOutTests : RavenTestBase
    {
        public CdcSinkFanOutTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Sinks)]
        public async Task DeleteFanOut_FirstProcessorIgnoresDeletes_LaterProcessorGetsIntactValues()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            var config = new CdcSinkConfiguration
            {
                Name = "fanout-delete",
                Tables = new List<CdcSinkTableConfig>
                {
                    CreateOrdersTableEmbeddingLines("Lines"),
                    new CdcSinkTableConfig
                    {
                        CollectionName = "OrderLines",
                        SourceTableSchema = "public",
                        SourceTableName = "order_lines",
                        PrimaryKeyColumns = new List<string> { "line_id" },
                        OnDelete = new CdcSinkOnDeleteConfig { IgnoreDeletes = true },
                        Columns = new List<CdcColumnMapping>
                        {
                            new() { Column = "line_id", Name = "LineId" },
                            new() { Column = "order_id", Name = "OrderId" },
                            new() { Column = "product", Name = "Product" }
                        }
                    }
                }
            };

            using var process = new TestCdcSinkProcess(config, database);
            var docProcessor = process.TestDocumentProcessor;
            docProcessor.SetSourceColumnNames("public", "order_lines", new[] { "line_id", "order_id", "product" });

            var processors = docProcessor.GetProcessors("public", "order_lines");
            Assert.Equal(2, processors.Count);
            Assert.True(processors[0].IsRoot);
            Assert.True(processors[0].IgnoresDeletes);
            Assert.False(processors[1].IgnoresDeletes);

            var values = processors[0].RentValues();
            values[0] = 10;
            values[1] = 1;
            values[2] = "Widget";

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var (deletes, upserts) = process.RunAddDeleteEvents(processors, values, context);

                Assert.Empty(upserts);
                var op = Assert.Single(deletes);
                Assert.Equal(CdcSinkDocumentOpType.EmbeddedModify, op.Type);
                Assert.Equal(CdcSinkOperation.Delete, op.Operation);
                Assert.Equal("Orders/1", op.DocumentId);
                Assert.NotSame(values, op.RawValues);
                Assert.Equal(10, (int)op.RawValues[0]);

                Assert.All(values, Assert.Null);
            }
        }

        [RavenFact(RavenTestCategory.Sinks)]
        public async Task UpdateFanOut_TwoEmbeddedMappings_NoReparent_EmitsOnlyUpserts()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            using var process = new TestCdcSinkProcess(CreateTwoEmbeddedMappingsConfig(), database);
            var docProcessor = process.TestDocumentProcessor;
            docProcessor.SetSourceColumnNames("public", "order_lines", new[] { "line_id", "order_id", "product" });

            var processors = docProcessor.GetProcessors("public", "order_lines");
            Assert.Equal(2, processors.Count);

            var newValues = processors[0].RentValues();
            newValues[0] = 10;
            newValues[1] = 1;
            newValues[2] = "Widget";
            var oldValues = processors[0].RentValues();
            oldValues[0] = 10;
            oldValues[1] = 1;
            oldValues[2] = "Gadget";

            var (deletes, upserts) = process.RunAddUpdateEvents(processors, newValues, oldValues);

            Assert.Empty(deletes);
            Assert.Equal(2, upserts.Count);
            Assert.All(upserts, op => Assert.Equal("Orders/1", op.DocumentId));

            Assert.All(oldValues, Assert.Null);
        }

        [RavenFact(RavenTestCategory.Sinks)]
        public async Task UpdateFanOut_TwoEmbeddedMappings_Reparent_BothDeletesTargetOldParent()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            using var process = new TestCdcSinkProcess(CreateTwoEmbeddedMappingsConfig(), database);
            var docProcessor = process.TestDocumentProcessor;
            docProcessor.SetSourceColumnNames("public", "order_lines", new[] { "line_id", "order_id", "product" });

            var processors = docProcessor.GetProcessors("public", "order_lines");

            var newValues = processors[0].RentValues();
            newValues[0] = 10;
            newValues[1] = 1;
            newValues[2] = "Widget";
            var oldValues = processors[0].RentValues();
            oldValues[0] = 10;
            oldValues[1] = 2;
            oldValues[2] = "Widget";

            var (deletes, upserts) = process.RunAddUpdateEvents(processors, newValues, oldValues);

            Assert.Equal(2, deletes.Count);
            Assert.All(deletes, op => Assert.Equal("Orders/2", op.DocumentId));
            Assert.NotSame(deletes[0].RawValues, deletes[1].RawValues);
            Assert.Equal(2, upserts.Count);
            Assert.All(upserts, op => Assert.Equal("Orders/1", op.DocumentId));
        }

        private static CdcSinkTableConfig CreateOrdersTableEmbeddingLines(params string[] propertyNames)
        {
            var embedded = new List<CdcSinkEmbeddedTableConfig>();
            foreach (var propertyName in propertyNames)
            {
                embedded.Add(new CdcSinkEmbeddedTableConfig
                {
                    SourceTableSchema = "public",
                    SourceTableName = "order_lines",
                    PropertyName = propertyName,
                    PrimaryKeyColumns = new List<string> { "line_id" },
                    JoinColumns = new List<string> { "order_id" },
                    Type = CdcSinkRelationType.Array,
                    Columns = new List<CdcColumnMapping>
                    {
                        new() { Column = "line_id", Name = "LineId" },
                        new() { Column = "product", Name = "Product" }
                    }
                });
            }

            return new CdcSinkTableConfig
            {
                CollectionName = "Orders",
                SourceTableSchema = "public",
                SourceTableName = "orders",
                PrimaryKeyColumns = new List<string> { "order_id" },
                Columns = new List<CdcColumnMapping> { new() { Column = "order_id", Name = "OrderId" } },
                EmbeddedTables = embedded
            };
        }

        private static CdcSinkConfiguration CreateTwoEmbeddedMappingsConfig()
        {
            return new CdcSinkConfiguration
            {
                Name = "fanout-update",
                Tables = new List<CdcSinkTableConfig> { CreateOrdersTableEmbeddingLines("Lines", "Items") }
            };
        }

        private sealed class TestCdcSinkProcess : CdcSinkProcess
        {
            public TestCdcSinkProcess(CdcSinkConfiguration configuration, DocumentDatabase database)
                : base(configuration, database, defaultSchema: "public")
            {
            }

            public CdcSinkDocumentProcessor TestDocumentProcessor => DocumentProcessor;

            public (List<CdcSinkDocumentOp> Deletes, List<CdcSinkDocumentOp> Upserts) RunAddDeleteEvents(
                IReadOnlyList<CdcSinkTableProcessor> processors, object[] values, JsonOperationContext context)
            {
                var events = new List<CdcEvent>();
                AddDeleteEvents(processors, values, context, events);
                return Split(events);
            }

            public (List<CdcSinkDocumentOp> Deletes, List<CdcSinkDocumentOp> Upserts) RunAddUpdateEvents(
                IReadOnlyList<CdcSinkTableProcessor> processors, object[] newValues, object[] oldValues)
            {
                var events = new List<CdcEvent>();
                AddUpdateEvents(processors, newValues, oldValues, events);
                return Split(events);
            }

            private static (List<CdcSinkDocumentOp> Deletes, List<CdcSinkDocumentOp> Upserts) Split(List<CdcEvent> events)
            {
                var deletes = new List<CdcSinkDocumentOp>();
                var upserts = new List<CdcSinkDocumentOp>();
                foreach (var e in events)
                {
                    if (e.Type == CdcEventType.Delete)
                        deletes.Add(e.Op);
                    else
                        upserts.Add(e.Op);
                }
                return (deletes, upserts);
            }

            public override bool IsHealthy(out string issue)
            {
                issue = null;
                return true;
            }

            protected override Task RunInternalAsync(CancellationToken ct) => throw new NotSupportedException();

            protected override IAsyncEnumerable<CdcEvent> GetCdcEvents(CancellationToken ct) => throw new NotSupportedException();

            protected override string GetDefaultSchema() => "public";

            protected override Task<DbConnection> OpenInitialLoadConnection(CancellationToken ct) => throw new NotSupportedException();

            protected override Task<List<string>> ResolveInitialLoadKeyColumnsAsync(DbConnection conn, string schema, string table, CancellationToken ct) => throw new NotSupportedException();

            protected override Task BindKeysetParameters(DbCommand cmd, CdcSinkConfiguration.TableInfo tableInfo, List<string> pkColumns, string[] lastKeys, CancellationToken ct) => throw new NotSupportedException();

            protected override object ConvertInitialLoadValue(DbDataReader reader, int ordinal, CdcSinkConfiguration.TableInfo tableInfo) => throw new NotSupportedException();

            protected override DbCommandBuilder CommandBuilder => null;

            public override void Dispose()
            {
            }
        }
    }
}

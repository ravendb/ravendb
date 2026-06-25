using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Server.Documents.CdcSink;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.CdcSink
{
    public class CdcSinkDisabledTableTests : RavenTestBase
    {
        public CdcSinkDisabledTableTests(ITestOutputHelper output) : base(output)
        {
        }

        // Two root tables, each with an embedded table. The "products" root can be toggled disabled.
        private static CdcSinkConfiguration BuildConfig(bool productsDisabled, string productsPatch = null)
        {
            return new CdcSinkConfiguration
            {
                Name = "test-config",
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Orders",
                        SourceTableSchema = "public",
                        SourceTableName = "orders",
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "order_id", Name = "OrderId" },
                            new CdcColumnMapping { Column = "customer_name", Name = "CustomerName" }
                        },
                        PrimaryKeyColumns = new List<string> { "order_id" },
                        EmbeddedTables = new List<CdcSinkEmbeddedTableConfig>
                        {
                            new CdcSinkEmbeddedTableConfig
                            {
                                SourceTableSchema = "public",
                                SourceTableName = "order_lines",
                                PropertyName = "Lines",
                                Columns = new List<CdcColumnMapping>
                                {
                                    new CdcColumnMapping { Column = "line_id", Name = "LineId" }
                                },
                                PrimaryKeyColumns = new List<string> { "line_id" },
                                JoinColumns = new List<string> { "order_id" },
                                Type = CdcSinkRelationType.Array
                            }
                        }
                    },
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Products",
                        SourceTableSchema = "public",
                        SourceTableName = "products",
                        Disabled = productsDisabled,
                        Patch = productsPatch,
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "product_id", Name = "ProductId" }
                        },
                        PrimaryKeyColumns = new List<string> { "product_id" },
                        EmbeddedTables = new List<CdcSinkEmbeddedTableConfig>
                        {
                            new CdcSinkEmbeddedTableConfig
                            {
                                SourceTableSchema = "public",
                                SourceTableName = "product_tags",
                                PropertyName = "Tags",
                                Columns = new List<CdcColumnMapping>
                                {
                                    new CdcColumnMapping { Column = "tag_id", Name = "TagId" }
                                },
                                PrimaryKeyColumns = new List<string> { "tag_id" },
                                JoinColumns = new List<string> { "product_id" },
                                Type = CdcSinkRelationType.Array
                            }
                        }
                    }
                }
            };
        }

        [RavenFact(RavenTestCategory.Sinks)]
        public void CollectAllTablesFlat_ExcludesDisabledRootTableAndItsEmbeddedTables()
        {
            // Initial load and every provider's change-capture setup enumerate tables through
            // CollectAllTablesFlat, so a disabled table must not appear there.
            var disabled = BuildConfig(productsDisabled: true).CollectAllTablesFlat("public")
                .Select(t => t.TableName).ToHashSet(StringComparer.OrdinalIgnoreCase);

            Assert.Contains("orders", disabled);
            Assert.Contains("order_lines", disabled);
            Assert.DoesNotContain("products", disabled);
            Assert.DoesNotContain("product_tags", disabled);

            var enabled = BuildConfig(productsDisabled: false).CollectAllTablesFlat("public")
                .Select(t => t.TableName).ToHashSet(StringComparer.OrdinalIgnoreCase);

            Assert.Contains("products", enabled);
            Assert.Contains("product_tags", enabled);
        }

        [RavenFact(RavenTestCategory.Sinks)]
        public void DocumentProcessor_DoesNotRegisterDisabledTable()
        {
            var processor = new CdcSinkDocumentProcessor(BuildConfig(productsDisabled: true), "public");

            Assert.True(processor.TryGetProcessor("public", "orders", out _));
            Assert.True(processor.TryGetProcessor("public", "order_lines", out _));

            Assert.False(processor.TryGetProcessor("public", "products", out _));
            Assert.False(processor.TryGetProcessor("public", "product_tags", out _));
            Assert.Throws<InvalidOperationException>(() => processor.GetProcessor("public", "products"));
        }

        [RavenFact(RavenTestCategory.Sinks)]
        public void DocumentProcessor_RegistersDisabledTable_WhenIncludeDisabledTablesRequested()
        {
            // Replay (CdcSinkBatchCommand) and the test/preview endpoint must resolve every configured
            // table regardless of state, so they opt back in via includeDisabledTables.
            var processor = new CdcSinkDocumentProcessor(BuildConfig(productsDisabled: true), "public", includeDisabledTables: true);

            Assert.True(processor.TryGetProcessor("public", "products", out _));
            Assert.True(processor.TryGetProcessor("public", "product_tags", out _));
            Assert.NotNull(processor.GetProcessor("public", "products"));
        }

        [RavenFact(RavenTestCategory.Sinks)]
        public void ProcessRow_DiscardsRowForDisabledTable()
        {
            var processor = new CdcSinkDocumentProcessor(BuildConfig(productsDisabled: true), "public");
            using var context = JsonOperationContext.ShortTermSingleUse();

            var row = new CdcSinkRow
            {
                TableSchema = "public",
                TableName = "products",
                Operation = CdcSinkOperation.Upsert,
                Data = new object[] { 1 }
            };

            // A streamed row for a disabled table resolves to no processor and is dropped.
            Assert.Null(processor.ProcessRow(row, context));
        }

        [RavenFact(RavenTestCategory.Sinks)]
        public void CombinedPatchRequest_ExcludesDisabledTablePatch()
        {
            const string patch = "this.Touched = true;";

            // Only the disabled table has a patch → nothing to dispatch at runtime.
            var runtime = new CdcSinkDocumentProcessor(BuildConfig(productsDisabled: true, productsPatch: patch), "public");
            Assert.Null(runtime.CombinedPatchRequest);

            // Replay/preview keep the patch so already-created ops can still dispatch it.
            var replay = new CdcSinkDocumentProcessor(BuildConfig(productsDisabled: true, productsPatch: patch), "public", includeDisabledTables: true);
            Assert.NotNull(replay.CombinedPatchRequest);

            // When enabled, the patch is available for runtime too.
            var enabled = new CdcSinkDocumentProcessor(BuildConfig(productsDisabled: false, productsPatch: patch), "public");
            Assert.NotNull(enabled.CombinedPatchRequest);
        }
    }
}

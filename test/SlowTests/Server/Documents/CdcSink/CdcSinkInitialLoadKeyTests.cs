using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Server.Documents.CdcSink;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.CdcSink
{
    public class CdcSinkInitialLoadKeyTests : RavenTestBase
    {
        public CdcSinkInitialLoadKeyTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Sinks)]
        public void CollectAllTablesFlat_EnumeratesRootAndEmbeddedTables()
        {
            var config = new CdcSinkConfiguration
            {
                Name = "t",
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Orders",
                        SourceTableSchema = "public",
                        SourceTableName = "orders",
                        Columns = new List<CdcColumnMapping> { new CdcColumnMapping { Column = "order_id", Name = "OrderId" } },
                        PrimaryKeyColumns = new List<string> { "order_id" },
                        EmbeddedTables = new List<CdcSinkEmbeddedTableConfig>
                        {
                            new CdcSinkEmbeddedTableConfig
                            {
                                SourceTableSchema = "public",
                                SourceTableName = "order_details",
                                PropertyName = "Lines",
                                Columns = new List<CdcColumnMapping> { new CdcColumnMapping { Column = "product_id", Name = "ProductId" } },
                                PrimaryKeyColumns = new List<string> { "product_id" },
                                JoinColumns = new List<string> { "order_id" },
                                Type = CdcSinkRelationType.Array
                            }
                        }
                    }
                }
            };

            var tables = config.CollectAllTablesFlat("public");

            Assert.Equal(new[] { "public.orders", "public.order_details" }, tables.Select(t => t.FullName).ToArray());
        }

        [RavenFact(RavenTestCategory.Sinks)]
        public void ResumeKeyColumnsMatch_SameColumns_Matches()
        {
            Assert.True(CdcSinkProcess.ResumeKeyColumnsMatch(
                new List<string> { "order_id", "product_id" },
                new List<string> { "order_id", "product_id" }));
        }

        [RavenFact(RavenTestCategory.Sinks)]
        public void ResumeKeyColumnsMatch_IsCaseInsensitive()
        {
            Assert.True(CdcSinkProcess.ResumeKeyColumnsMatch(
                new List<string> { "Order_Id" },
                new List<string> { "order_id" }));
        }

        [RavenFact(RavenTestCategory.Sinks)]
        public void ResumeKeyColumnsMatch_DifferentColumnsSameCount_DoesNotMatch()
        {
            Assert.False(CdcSinkProcess.ResumeKeyColumnsMatch(
                new List<string> { "order_id" },
                new List<string> { "line_id" }));
        }

        [RavenFact(RavenTestCategory.Sinks)]
        public void ResumeKeyColumnsMatch_NullPersisted_DoesNotMatch()
        {
            Assert.False(CdcSinkProcess.ResumeKeyColumnsMatch(null, new List<string> { "order_id" }));
        }

        [RavenFact(RavenTestCategory.Sinks)]
        public void ResumeKeyColumnsMatch_DifferentCount_DoesNotMatch()
        {
            Assert.False(CdcSinkProcess.ResumeKeyColumnsMatch(
                new List<string> { "order_id" },
                new List<string> { "order_id", "product_id" }));
        }

        [RavenFact(RavenTestCategory.Sinks)]
        public void AddDiscoveredPrimaryKeyColumn_BucketsByTableAndPreservesOrder()
        {
            var map = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
            
            CdcSinkProcess.AddDiscoveredPrimaryKeyColumn(map, "public", "order_details", "order_id");
            CdcSinkProcess.AddDiscoveredPrimaryKeyColumn(map, "public", "orders", "order_id");
            CdcSinkProcess.AddDiscoveredPrimaryKeyColumn(map, "public", "order_details", "product_id");

            Assert.Equal(new[] { "order_id", "product_id" }, map["public.order_details"]);
            Assert.Equal(new[] { "order_id" }, map["public.orders"]);
        }

        [RavenFact(RavenTestCategory.Sinks)]
        public void ChooseInitialLoadKey_PrefersPrimaryKeyOverUniqueKey()
        {
            var uniques = new Dictionary<string, Dictionary<string, CdcSinkProcess.UniqueKeyAccumulator>>(StringComparer.OrdinalIgnoreCase);
            CdcSinkProcess.AddDiscoveredUniqueKeyColumn(uniques, "public", "users", "uq_email", "email", columnNotNull: true);

            var chosen = CdcSinkProcess.ChooseInitialLoadKey(new List<string> { "id" }, uniques["public.users"]);

            Assert.Equal(new[] { "id" }, chosen);
        }

        [RavenFact(RavenTestCategory.Sinks)]
        public void ChooseInitialLoadKey_NoPrimaryKey_UsesNotNullUniqueKey()
        {
            var uniques = new Dictionary<string, Dictionary<string, CdcSinkProcess.UniqueKeyAccumulator>>(StringComparer.OrdinalIgnoreCase);
            CdcSinkProcess.AddDiscoveredUniqueKeyColumn(uniques, "public", "users", "uq_email", "email", columnNotNull: true);

            var chosen = CdcSinkProcess.ChooseInitialLoadKey(primaryKey: null, uniques["public.users"]);

            Assert.Equal(new[] { "email" }, chosen);
        }

        [RavenFact(RavenTestCategory.Sinks)]
        public void ChooseInitialLoadKey_PicksFewestColumnUniqueKey()
        {
            var uniques = new Dictionary<string, Dictionary<string, CdcSinkProcess.UniqueKeyAccumulator>>(StringComparer.OrdinalIgnoreCase);
            // A two-column unique key and a single-column unique key, both fully NOT NULL.
            CdcSinkProcess.AddDiscoveredUniqueKeyColumn(uniques, "public", "t", "uq_a_b", "a", columnNotNull: true);
            CdcSinkProcess.AddDiscoveredUniqueKeyColumn(uniques, "public", "t", "uq_a_b", "b", columnNotNull: true);
            CdcSinkProcess.AddDiscoveredUniqueKeyColumn(uniques, "public", "t", "uq_c", "c", columnNotNull: true);

            var chosen = CdcSinkProcess.ChooseInitialLoadKey(primaryKey: null, uniques["public.t"]);

            Assert.Equal(new[] { "c" }, chosen);
        }

        [RavenFact(RavenTestCategory.Sinks)]
        public void ChooseInitialLoadKey_SkipsUniqueKeyWithNullableColumn()
        {
            var uniques = new Dictionary<string, Dictionary<string, CdcSinkProcess.UniqueKeyAccumulator>>(StringComparer.OrdinalIgnoreCase);
            // The only unique key has a nullable column → unusable → no key.
            CdcSinkProcess.AddDiscoveredUniqueKeyColumn(uniques, "public", "t", "uq_email", "email", columnNotNull: false);

            Assert.Null(CdcSinkProcess.ChooseInitialLoadKey(primaryKey: null, uniques["public.t"]));
        }

        [RavenFact(RavenTestCategory.Sinks)]
        public void ChooseInitialLoadKey_NoKeys_ReturnsNull()
        {
            Assert.Null(CdcSinkProcess.ChooseInitialLoadKey(primaryKey: null, uniqueKeys: null));
            Assert.Null(CdcSinkProcess.ChooseInitialLoadKey(new List<string>(), uniqueKeys: null));
        }
    }
}

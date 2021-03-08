using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents.Operations;
using Raven.Server.ServerWide.Context;
using Raven.Server.SqlMigration;
using Raven.Server.SqlMigration.Model;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.Migration
{
    public class RecursiveMigrationTest : SqlAwareTestBase
    {
        public RecursiveMigrationTest(ITestOutputHelper output) : base(output)
        {
        }

        [NightlyBuildTheory]
        [InlineData(MigrationProvider.MsSQL)]
        [RequiresMySqlInlineData]
        [RequiresNpgSqlInlineData]
        [RequiresOracleSqlInlineData]
        public async Task CanLinkOnParent(MigrationProvider provider)
        {
            const string tableName = "groups1";
            const string collectionName = "Groups1";

            using (WithSqlDatabase(provider, out var connectionString, out string schemaName, "basic"))
            {
                var driver = DatabaseDriverDispatcher.CreateDriver(provider, connectionString);
                using (var store = GetDocumentStore())
                {
                    var db = await GetDocumentDatabaseInstanceFor(store);

                    var settings = new MigrationSettings
                    {
                        Collections = new List<RootCollection>
                        {
                            new RootCollection(schemaName, tableName, collectionName)
                            {
                                LinkedCollections = new List<LinkedCollection>
                                {
                                    new LinkedCollection(schemaName, tableName, RelationType.ManyToOne, new List<string> { "parent_group_id" }, "Parent")
                                }
                            }
                        }
                    };

                    using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(1)))
                    using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    {
                        var schema = driver.FindSchema();
                        ApplyDefaultColumnNamesMapping(schema, settings);
                        await driver.Migrate(settings, schema, db, context, token: cts.Token);
                    }

                    using (var session = store.OpenSession())
                    {
                        var g1111 = session.Load<JObject>($"{collectionName}/53");
                        Assert.Equal("G1.1.1.1", g1111["Name"]);
                        var g111 = session.Load<JObject>(g1111["Parent"].ToString());
                        Assert.Equal("G1.1.1", g111["Name"]);
                        var g11 = session.Load<JObject>(g111["Parent"].ToString());
                        Assert.Equal("G1.1", g11["Name"]);
                    }

                    var collectionStatistics = store.Maintenance.Send(new GetCollectionStatisticsOperation());
                    Assert.Equal(7, collectionStatistics.CountOfDocuments);
                }
            }
        }

        [NightlyBuildTheory]
        [InlineData(MigrationProvider.MsSQL)]
        [RequiresMySqlInlineData]
        [RequiresNpgSqlInlineData]
        [RequiresOracleSqlInlineData]
        public async Task CanLinkOnChild(MigrationProvider provider)
        {
            var tableName = "groups1";
            var collectionName = "Groups1";

            using (WithSqlDatabase(provider, out var connectionString, out string schemaName, "basic"))
            {
                var driver = DatabaseDriverDispatcher.CreateDriver(provider, connectionString);
                using (var store = GetDocumentStore())
                {
                    var db = await GetDocumentDatabaseInstanceFor(store);

                    var settings = new MigrationSettings
                    {
                        Collections = new List<RootCollection>
                        {
                            new RootCollection(schemaName, tableName, collectionName)
                            {
                                LinkedCollections = new List<LinkedCollection>
                                {
                                    new LinkedCollection(schemaName, tableName, RelationType.OneToMany, new List<string> { "parent_group_id" }, "NestedGroups")
                                }
                            }
                        }
                    };

                    using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(1)))
                    using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    {
                        var schema = driver.FindSchema();
                        ApplyDefaultColumnNamesMapping(schema, settings);
                        await driver.Migrate(settings, schema, db, context, token: cts.Token);
                    }

                    using (var session = store.OpenSession())
                    {
                        var g11 = session.Load<JObject>($"{collectionName}/51");
                        Assert.Equal(2, g11["NestedGroups"].Count());
                        Assert.Equal($"{collectionName}/52", g11["NestedGroups"][0]);
                        Assert.Equal($"{collectionName}/54", g11["NestedGroups"][1]);
                    }

                    var collectionStatistics = store.Maintenance.Send(new GetCollectionStatisticsOperation());
                    Assert.Equal(7, collectionStatistics.CountOfDocuments);
                }
            }
        }

        [NightlyBuildTheory]
        [InlineData(MigrationProvider.MsSQL)]
        [RequiresMySqlInlineData]
        [RequiresNpgSqlInlineData]
        [RequiresOracleSqlInlineData]
        public async Task CanEmbedOnParent(MigrationProvider provider)
        {
            const string tableName = "groups1";
            const string collectionName = "Groups1";

            using (WithSqlDatabase(provider, out var connectionString, out string schemaName, "basic"))
            {
                var driver = DatabaseDriverDispatcher.CreateDriver(provider, connectionString);
                using (var store = GetDocumentStore())
                {
                    var db = await GetDocumentDatabaseInstanceFor(store);

                    var settings = new MigrationSettings
                    {
                        Collections = new List<RootCollection>
                        {
                            new RootCollection(schemaName, tableName, collectionName)
                            {
                                NestedCollections = new List<EmbeddedCollection>
                                {
                                    new EmbeddedCollection(schemaName, tableName, RelationType.ManyToOne, new List<string> { "parent_group_id" }, "Parent")
                                    {
                                        NestedCollections = new List<EmbeddedCollection>
                                        {
                                            new EmbeddedCollection(schemaName, tableName, RelationType.ManyToOne, new List<string> { "parent_group_id" }, "Grandparent")
                                        }
                                    }
                                }
                            }
                        }
                    };

                    using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(1)))
                    using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    {
                        var schema = driver.FindSchema();
                        ApplyDefaultColumnNamesMapping(schema, settings);
                        await driver.Migrate(settings, schema, db, context, token: cts.Token);
                    }

                    using (var session = store.OpenSession())
                    {
                        var group = session.Load<JObject>($"{collectionName}/53");
                        Assert.Equal("G1.1.1.1", group["Name"]);
                        var parent = group["Parent"];
                        Assert.NotNull(parent);
                        Assert.Equal("G1.1.1", parent["Name"]);
                        var grandparent = parent["Grandparent"];
                        Assert.NotNull(grandparent);
                        Assert.Equal("G1.1", grandparent["Name"]);
                    }

                    var collectionStatistics = store.Maintenance.Send(new GetCollectionStatisticsOperation());
                    Assert.Equal(7, collectionStatistics.CountOfDocuments);
                }
            }
        }
    }
}

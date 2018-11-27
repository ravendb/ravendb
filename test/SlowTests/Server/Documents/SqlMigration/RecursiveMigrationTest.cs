using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents.Operations;
using Raven.Server.ServerWide.Context;
using Raven.Server.SqlMigration;
using Raven.Server.SqlMigration.Model;
using Sparrow.Json.Parsing;
using Xunit;

namespace SlowTests.Server.Documents.SqlMigration
{
    public class RecursiveMigrationTest : SqlAwareTestBase
    {
        [Theory]
        [InlineData(MigrationProvider.MsSQL)]
        [RequiresMySqlInlineData]
        [RequiresNpgSqlInlineData]
        [RequiresOracleSqlInlineData]
        public async Task CanLinkOnParent(MigrationProvider provider)
        {
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
                            new RootCollection(schemaName, "groups", "Groups")
                            {
                                LinkedCollections = new List<LinkedCollection>
                                {
                                    new LinkedCollection(schemaName, "groups", RelationType.ManyToOne, new List<string> { "parent_group_id" }, "Parent")
                                }
                            }
                        }
                    };

                    using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    {
                        var schema = driver.FindSchema();
                        ApplyDefaultColumnNamesMapping(schema, settings);
                        await driver.Migrate(settings, schema, db, context);
                    }

                    using (var session = store.OpenSession())
                    {
                        var g1111 = session.Load<JObject>("Groups/53");
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
        
        [Theory]
        [InlineData(MigrationProvider.MsSQL)]
        [RequiresMySqlInlineData]
        [RequiresNpgSqlInlineData]
        [RequiresOracleSqlInlineData]
        public async Task CanLinkOnChild(MigrationProvider provider)
        {
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
                            new RootCollection(schemaName, "groups", "Groups")
                            {
                                LinkedCollections = new List<LinkedCollection>
                                {
                                    new LinkedCollection(schemaName, "groups", RelationType.OneToMany, new List<string> { "parent_group_id" }, "NestedGroups")
                                }
                            }
                        }
                    };

                    using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    {
                        var schema = driver.FindSchema();
                        ApplyDefaultColumnNamesMapping(schema, settings);
                        await driver.Migrate(settings, schema, db, context);
                    }

                    using (var session = store.OpenSession())
                    {
                        var g11 = session.Load<JObject>("Groups/51");
                        Assert.Equal(2, g11["NestedGroups"].Count());
                        Assert.Equal("Groups/52", g11["NestedGroups"][0]);
                        Assert.Equal("Groups/54", g11["NestedGroups"][1]);
                    }

                    var collectionStatistics = store.Maintenance.Send(new GetCollectionStatisticsOperation());
                    Assert.Equal(7, collectionStatistics.CountOfDocuments);
                }
            }
        }
        
        [Theory]
        [InlineData(MigrationProvider.MsSQL)]
        [RequiresMySqlInlineData]
        [RequiresNpgSqlInlineData]
        [RequiresOracleSqlInlineData]
        public async Task CanEmbedOnParent(MigrationProvider provider)
        {
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
                            new RootCollection(schemaName, "groups", "Groups")
                            {
                                NestedCollections = new List<EmbeddedCollection>
                                {
                                    new EmbeddedCollection(schemaName, "groups", RelationType.ManyToOne, new List<string> { "parent_group_id" }, "Parent")
                                    {
                                        NestedCollections = new List<EmbeddedCollection>
                                        {
                                            new EmbeddedCollection(schemaName, "groups", RelationType.ManyToOne, new List<string> { "parent_group_id" }, "Grandparent")
                                        }
                                    }
                                }
                            }
                        }
                    };

                    using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    {
                        var schema = driver.FindSchema();
                        ApplyDefaultColumnNamesMapping(schema, settings);
                        await driver.Migrate(settings, schema, db, context);
                    }

                    using (var session = store.OpenSession())
                    {
                        var group = session.Load<JObject>("Groups/53");
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

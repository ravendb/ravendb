using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json.Linq;
using Raven.Server.ServerWide.Context;
using Raven.Server.SqlMigration;
using Raven.Server.SqlMigration.Model;
using Sparrow.Json;
using Xunit;

namespace SlowTests.Server.Documents.SqlMigration
{
    public class TestMigrationTest : SqlAwareTestBase
    {
        [Theory]
        [InlineData(MigrationProvider.MsSQL)]
        [RequiresMySqlInlineData]
        [RequiresNpgSqlInlineData]
        [RequiresOracleSqlInlineData]
        public async Task CanTestWithEmbed(MigrationProvider provider)
        {
            using (WithSqlDatabase(provider, out var connectionString, out string schemaName, "basic"))
            {
                var driver = DatabaseDriverDispatcher.CreateDriver(provider, connectionString);
                using (var store = GetDocumentStore())
                {
                    var db = await GetDocumentDatabaseInstanceFor(store);

                    var settings = new MigrationTestSettings
                    {
                        Collection = new RootCollection(schemaName, "order", "Orders")
                        {
                            NestedCollections = new List<EmbeddedCollection>
                            {
                                 new EmbeddedCollection(schemaName, "order_item", RelationType.OneToMany, new List<string> { "order_id" }, "Items")
                            }
                        }
                    };

                    using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    {
                        var schema = driver.FindSchema();
                        ApplyDefaultColumnNamesMapping(schema, settings.Collection, settings.BinaryToAttachment);
                        var (document, id) = driver.Test(settings, schema, context);
                        
                        Assert.Equal("Orders/1", id);
                        Assert.True(document.TryGet("Total", out double total));
                        Assert.Equal(440, total);
                        
                        var items = document["Items"] as BlittableJsonReaderArray;
                        Assert.NotNull(items);
                        Assert.Equal(2, items.Length);
                    }
                }
            }
        }
        
        [Theory]
        [InlineData(MigrationProvider.MsSQL)]
        [RequiresMySqlInlineData]
        [RequiresNpgSqlInlineData]
        [RequiresOracleSqlInlineData]
        public async Task CanTestWithSkip(MigrationProvider provider)
        {
            using (WithSqlDatabase(provider, out var connectionString, out string schemaName, "basic"))
            {
                var driver = DatabaseDriverDispatcher.CreateDriver(provider, connectionString);
                using (var store = GetDocumentStore())
                {
                    var db = await GetDocumentDatabaseInstanceFor(store);

                    var settings = new MigrationTestSettings
                    {
                        Collection = new RootCollection(schemaName, "order", "Orders")
                        {
                            Patch = "throw 'skip';"
                        }
                    };

                    using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    {
                        var schema = driver.FindSchema();
                        ApplyDefaultColumnNamesMapping(schema, settings.Collection, settings.BinaryToAttachment);
                        
                        var exception = Assert.Throws<InvalidOperationException>(() =>
                        {
                            driver.Test(settings, schema, context);
                        });
                        
                        Assert.True(exception.Message.StartsWith("Document was skipped"));
                    }
                }
            }
        }
        
        [Theory]
        [InlineData(MigrationProvider.MsSQL)]
        [RequiresMySqlInlineData]
        [RequiresNpgSqlInlineData]
        [RequiresOracleSqlInlineData]
        public async Task CanTestWithPrimaryKeyValues(MigrationProvider provider)
        {
            using (WithSqlDatabase(provider, out var connectionString, out string schemaName, "basic"))
            {
                var driver = DatabaseDriverDispatcher.CreateDriver(provider, connectionString);
                using (var store = GetDocumentStore())
                {
                    var db = await GetDocumentDatabaseInstanceFor(store);

                    var settings = new MigrationTestSettings
                    {
                        Collection = new RootCollection(schemaName, "groups", "Groups"),
                        Mode = MigrationTestMode.ByPrimaryKey,
                        PrimaryKeyValues = new [] { "52" }
                    };

                    using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    {
                        var schema = driver.FindSchema();
                        ApplyDefaultColumnNamesMapping(schema, settings.Collection, settings.BinaryToAttachment);
                        var (document, id) = driver.Test(settings, schema, context);
                        
                        Assert.Equal("Groups/52", id);
                        Assert.True(document.TryGet("Name", out string name));
                        Assert.Equal("G1.1.1", name);
                    }
                }
            }
        }
    }
}

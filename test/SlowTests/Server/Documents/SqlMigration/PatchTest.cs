using System.Collections.Generic;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using Raven.Server.ServerWide.Context;
using Raven.Server.SqlMigration;
using Raven.Server.SqlMigration.Model;
using Xunit;

namespace SlowTests.Server.Documents.SqlMigration
{
    public class PatchTest : SqlAwareTestBase
    {
        [Theory]
        [InlineData(MigrationProvider.MsSQL)]
        [InlineData(MigrationProvider.MySQL)]
        public async Task SimplePatch(MigrationProvider provider)
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
                            new RootCollection(schemaName, "order", "Orders")
                            {
                                Patch = "this.NewField = 5;"
                            }
                        }
                    };

                    using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    {
                        var schema = driver.FindSchema();
                        await driver.Migrate(settings, schema, db, context);
                    }

                    using (var session = store.OpenSession())
                    {
                        var order = session.Load<JObject>("Orders/1");
                        Assert.NotNull(order);

                        Assert.Equal(5, order["NewField"]);
                    }
                }
            }
        }

        [Fact] //TODO: use theory
        public async Task PatchCanAccessNestedObjects()
        {
            using (WithSqlDatabase(MigrationProvider.MsSQL, out var connectionString, out string schemaName, "basic"))
            {
                var driver = DatabaseDriverDispatcher.CreateDriver(MigrationProvider.MsSQL, connectionString);
                using (var store = GetDocumentStore())
                {
                    var db = await GetDocumentDatabaseInstanceFor(store);

                    var settings = new MigrationSettings
                    {
                        Collections = new List<RootCollection>
                        {
                            new RootCollection(schemaName, "order", "Orders")
                            {
                                Patch = "this.JsTotal = this.Items.map(x => x.price).reduce((acc, cur) => acc + cur, 0)",
                                NestedCollections = new List<EmbeddedCollection>
                                {
                                    new EmbeddedCollection(schemaName, "order_item", "Items")
                                }
                            }
                        }
                    };

                    using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    {
                        var schema = driver.FindSchema();
                        await driver.Migrate(settings, schema, db, context);
                    }

                    using (var session = store.OpenSession())
                    {
                        var order = session.Load<JObject>("Orders/1");

                        Assert.NotNull(order);
                        Assert.Equal(440, order["JsTotal"]);
                    }
                }
            }
        }

        //TODO: test for throw 'skip'
    }
}

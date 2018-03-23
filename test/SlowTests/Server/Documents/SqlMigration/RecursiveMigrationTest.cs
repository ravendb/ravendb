using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
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
        [InlineData(MigrationProvider.MySQL)]
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
                                    new LinkedCollection(schemaName, "groups", "Parent", new List<string> { "parent_group_id" })
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
                        WaitForUserToContinueTheTest(store);
                        
                        //TODO: asserts
                    }

                    var collectionStatistics = store.Maintenance.Send(new GetCollectionStatisticsOperation());
                    Assert.Equal(7, collectionStatistics.CountOfDocuments);
                }
            }
        }
    }
}

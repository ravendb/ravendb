using System;
using System.IO;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Smuggler;
using Raven.Server.ServerWide.Context;
using SlowTests.Core.Utils.Entities;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15898 : RavenTestBase
    {
        public RavenDB_15898(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task DeleteDocumentWithResolvedFlag()
        {
            using (var store = GetDocumentStore())
            {
                using (var stream = GetDump("RavenDB_15898.1.ravendbdump"))
                {
                    var operation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), stream);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                }

                using (var session = store.OpenSession())
                {
                    session.Delete("users/1");
                    session.SaveChanges();
                }

                var database = await GetDatabase(store.Database);
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                {
                    context.OpenReadTransaction();
                    var count = database.DocumentsStorage.RevisionsStorage.GetNumberOfRevisionDocuments(context);
                    Assert.Equal(0, count);
                }
            }
        }

        [Fact(Skip = "RavenDB-16051")]
        public async Task DeleteDocumentWithResolvedFlagAfterEnableRevisions()
        {
            using (var store = GetDocumentStore())
            {
                using (var stream = GetDump("RavenDB_15898.1.ravendbdump"))
                {
                    var operation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), stream);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                }
                var configuration = new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration
                    {
                        Disabled = false,
                        MinimumRevisionsToKeep = 5
                    }
                };
                var database = await GetDatabase(store.Database);
                using (var context = JsonOperationContext.ShortTermSingleUse())
                {
                    var configurationJson = EntityToBlittable.ConvertCommandToBlittable(configuration, context);
                    var (index, _) = await database.ServerStore.ModifyDatabaseRevisions(context, store.Database, configurationJson, Guid.NewGuid().ToString());
                    await database.RachisLogIndexNotifications.WaitForIndexNotification(index, database.ServerStore.Engine.OperationTimeout);
                }

                using (var session = store.OpenSession())
                {
                    session.Delete("users/1");
                    session.SaveChanges();
                }

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                {
                    context.OpenReadTransaction();
                    var count = database.DocumentsStorage.RevisionsStorage.GetNumberOfRevisionDocuments(context);
                    Assert.Equal(2, count);
                }
            }
        }

        [Fact]
        public async Task DeleteDocumentWithoutResolvedFlagAfterEnableRevisions()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User{Name = "Toli"}, "users/1");
                    session.SaveChanges();
                }

                var configuration = new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration
                    {
                        Disabled = false,
                        MinimumRevisionsToKeep = 5
                    }
                };
                var database = await GetDatabase(store.Database);
                using (var context = JsonOperationContext.ShortTermSingleUse())
                {
                    var configurationJson = EntityToBlittable.ConvertCommandToBlittable(configuration, context);
                    var (index, _) = await database.ServerStore.ModifyDatabaseRevisions(context, store.Database, configurationJson, Guid.NewGuid().ToString());
                    await database.RachisLogIndexNotifications.WaitForIndexNotification(index, database.ServerStore.Engine.OperationTimeout);
                }
                using (var session = store.OpenSession())
                {
                    session.Delete("users/1");
                    session.SaveChanges();
                }

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                {
                    context.OpenReadTransaction();
                    var count = database.DocumentsStorage.RevisionsStorage.GetNumberOfRevisionDocuments(context);
                    Assert.Equal(2, count);
                }
            }
        }

        private static Stream GetDump(string name)
        {
            var assembly = typeof(RavenDB_15898).Assembly;
            return assembly.GetManifestResourceStream("SlowTests.Data." + name);
        }

    }
}

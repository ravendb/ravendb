using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using FastTests.Graph;
using FastTests.Utils;
using Nest;
using Raven.Client;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Server.Documents;
using Raven.Server.Documents.Revisions;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Voron.Data.Tables;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_21381 : RavenTestBase
    {
        public RavenDB_21381(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task AdoptOrphanedRevisionsTest()
        {
            using var store = GetDocumentStore();
            var configuration = new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration
                {
                    Disabled = false,
                    MinimumRevisionsToKeep = 100
                }
            };
            await RevisionsHelper.SetupRevisions(store, Server.ServerStore, configuration: configuration);

            var user1 = new User { Id = "Users/1-A", Name = "Shahar" };
            var user2 = new User { Id = "Users/2-B", Name = "Shahar" };
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(user1);
                await session.StoreAsync(user2);
                await session.SaveChangesAsync();

                for (int i = 1; i <= 10; i++)
                {
                    (await session.LoadAsync<User>(user1.Id)).Name = $"Shahar{i}";
                    (await session.LoadAsync<User>(user2.Id)).Name = $"Shahar{i}";
                    await session.SaveChangesAsync();
                }

                session.Delete(user1.Id);
                session.Delete(user2.Id);
                await session.SaveChangesAsync();

                var user1RevCount = await session.Advanced.Revisions.GetCountForAsync(user1.Id);
                Assert.Equal(12, user1RevCount);
                var revisionsMetadata1 = await session.Advanced.Revisions.GetMetadataForAsync(user1.Id);
                Assert.Contains(DocumentFlags.DeleteRevision.ToString(), revisionsMetadata1[0].GetString(Constants.Documents.Metadata.Flags));

                var user2RevCount = await session.Advanced.Revisions.GetCountForAsync(user2.Id);
                Assert.Equal(12, user2RevCount);
                var revisionsMetadata2 = await session.Advanced.Revisions.GetMetadataForAsync(user2.Id);
                Assert.Contains(DocumentFlags.DeleteRevision.ToString(), revisionsMetadata2[0].GetString(Constants.Documents.Metadata.Flags));
            }

            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            // Delete the last revision (the 'Delete Revision')
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using(var tx = context.OpenWriteTransaction())
            {
                database.DocumentsStorage.RevisionsStorage.ForTestingPurposesOnly().DeleteLastRevisionFor(context, user1.Id, "Users");
                tx.Commit();
            }


            // Assert that last revision of user1 isn't delete revision anymore (we created 11 orphaned revisions of 'Users/1-A')
            using (var session = store.OpenAsyncSession())
            {
                var user1RevCount = await session.Advanced.Revisions.GetCountForAsync(user1.Id);
                Assert.Equal(11, user1RevCount);
                var revisionsMetadata1 = await session.Advanced.Revisions.GetMetadataForAsync(user1.Id);
                Assert.False(revisionsMetadata1[0].GetString(Constants.Documents.Metadata.Flags).Contains(DocumentFlags.DeleteRevision.ToString()));

                var user2RevCount = await session.Advanced.Revisions.GetCountForAsync(user2.Id);
                Assert.Equal(12, user2RevCount);
                var revisionsMetadata2 = await session.Advanced.Revisions.GetMetadataForAsync(user2.Id);
                Assert.Contains(DocumentFlags.DeleteRevision.ToString(), revisionsMetadata2[0].GetString(Constants.Documents.Metadata.Flags));
            }

            // Run AdoptOrphaned and assert that new 'Delete Revision' was created again for user1
            var token = new OperationCancelToken(database.Configuration.Databases.OperationTimeout.AsTimeSpan, database.DatabaseShutdown);
            await database.DocumentsStorage.RevisionsStorage.AdoptOrphanedAsync(null, token);
            using (var session = store.OpenAsyncSession())
            {
                var user1RevCount = await session.Advanced.Revisions.GetCountForAsync(user1.Id);
                Assert.Equal(12, user1RevCount);
                var revisionsMetadata1 = await session.Advanced.Revisions.GetMetadataForAsync(user1.Id);
                Assert.Contains(DocumentFlags.DeleteRevision.ToString(), revisionsMetadata1[0].GetString(Constants.Documents.Metadata.Flags));

                var user2RevCount = await session.Advanced.Revisions.GetCountForAsync(user2.Id);
                Assert.Equal(12, user2RevCount);
                var revisionsMetadata2 = await session.Advanced.Revisions.GetMetadataForAsync(user2.Id);
                Assert.Contains(DocumentFlags.DeleteRevision.ToString(), revisionsMetadata2[0].GetString(Constants.Documents.Metadata.Flags));
            }

        }
    }
}

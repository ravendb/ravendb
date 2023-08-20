using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using FastTests.Graph;
using FastTests.Server.Replication;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Server.Documents;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;
public class RavenDB_20423 : ReplicationTestBase
{
    public RavenDB_20423(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public async Task RevisionsWithAttachmentOfDeletedDocAreReplicated()
    {
        using (var source = GetDocumentStore())
        using (var destination = GetDocumentStore())
        {
            var configuration = new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration
                {
                    Disabled = false,
                    MaximumRevisionsToDeleteUponDocumentUpdate = 1,
                    MinimumRevisionsToKeep = 10,
                    PurgeOnDelete = false
                }
            };
            await RevisionsHelper.SetupRevisions(source, Server.ServerStore, configuration: configuration);

            var id = "FoObAr/0";
            using (var session = source.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "Foo" }, id);
                await session.SaveChangesAsync();
            }

            using (var session = source.OpenAsyncSession())
            using (var fooStream = new MemoryStream(new byte[] { 1, 2, 3 }))
            {
                session.Advanced.Attachments.Store(id, "foo.png", fooStream, "image/png");
                await session.SaveChangesAsync();
            }

            using (var session = source.OpenAsyncSession())
            {
                var doc = await session.LoadAsync<User>(id);
                doc.Age = 30;
                await session.SaveChangesAsync();
            }

            configuration.Default.Disabled = true;
            await RevisionsHelper.SetupRevisions(source, Server.ServerStore, configuration: configuration);


            using (var session = source.OpenAsyncSession())
            {
                session.Delete(id);
                await session.SaveChangesAsync();
            }

            var database = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(source.Database);
            await database.TombstoneCleaner.ExecuteCleanup();

            await SetupReplicationAsync(source, destination);
            await EnsureReplicatingAsync(source, destination);
            
            // WaitForUserToContinueTheTest(destination, false);

            using (var session = destination.OpenAsyncSession())
            {
                var doc = await session.LoadAsync<User>(id);
                Assert.Null(doc); // doc has been deleted, So it should load null (even its revisions exist in the revisions bin).

                // Deleted doc has 4 revisions, the last is 'Delete Revision'
                var revisionsCount = await session.Advanced.Revisions.GetCountForAsync(id);
                Assert.Equal(4, revisionsCount);

                var revisionsMetadata = await session.Advanced.Revisions.GetMetadataForAsync(id);
                Assert.Equal(4, revisionsMetadata.Count);
                Assert.Contains(DocumentFlags.DeleteRevision.ToString(), revisionsMetadata[0].GetString(Constants.Documents.Metadata.Flags));

                var att1 = revisionsMetadata[1].GetObjects("@attachments");
                var att2 = revisionsMetadata[2].GetObjects("@attachments");
                Assert.Equal(1, att1.Length);
                Assert.Equal("foo.png", att1[0]["Name"].ToString());
                Assert.Equal(1, att2.Length);
                Assert.Equal("foo.png", att2[0]["Name"].ToString());

                Assert.False(revisionsMetadata[0].Keys.Contains("@attachments")); // the last revision ('Delete Revision') doesn't contain any attachments/counters/time-series.
                Assert.False(revisionsMetadata[3].Keys.Contains("@attachments")); // the first revision was created before the attachment was stored, therefore it wont have any attachment.
            }
        }
    }
}


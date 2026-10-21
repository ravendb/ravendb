using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    // DeleteRevisionsInternal keeps the newest revision when it is a DeleteRevision and the document still has
    // others, "so the previous revisions that remained wont become orphan". The check behind that, RevisionIsLast,
    // is given a slice it treats as a key prefix: it cuts the trailing record separator to recover the id and uses
    // the slice itself as the prefix to seek by. ForceDeleteAllRevisionsFor used to hand it the plain lower id
    // instead, so the recovered id lost its last character and the prefix matched neighbouring ids as well - the
    // answer came back "not the last" almost always, and the guard never engaged on that path.
    public class RavenDB_27130_DeleteRevisionGuard : RavenTestBase
    {
        public RavenDB_27130_DeleteRevisionGuard(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Revisions)]
        public async Task DeletingOnlyTheDeleteRevisionKeepsIt()
        {
            using var store = GetDocumentStore();

            await RevisionsHelper.SetupRevisionsAsync(store, Server.ServerStore, new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration { Disabled = false, MinimumRevisionsToKeep = 100 }
            });

            const string id = "users/1";

            for (var i = 0; i < 3; i++)
            {
                using var session = store.OpenAsyncSession();
                await session.StoreAsync(new User { Name = "v" + i }, id);
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                session.Delete(id);
                await session.SaveChangesAsync();
            }

            var (deleteRevisionChangeVector, countBefore) = await GetDeleteRevisionAsync(store, id);

            // the document has three revisions besides the delete one, so removing it would leave them unreachable
            Assert.Equal(4, countBefore);
            Assert.NotNull(deleteRevisionChangeVector);

            await store.Maintenance.SendAsync(new DeleteRevisionsOperation(id, new List<string> { deleteRevisionChangeVector }));

            var (deleteRevisionAfter, countAfter) = await GetDeleteRevisionAsync(store, id);

            Assert.Equal(countBefore, countAfter);
            Assert.Equal(deleteRevisionChangeVector, deleteRevisionAfter);
        }

        private static async Task<(string DeleteRevisionChangeVector, int Count)> GetDeleteRevisionAsync(Raven.Client.Documents.IDocumentStore store, string id)
        {
            using var session = store.OpenAsyncSession();
            var metadata = await session.Advanced.Revisions.GetMetadataForAsync(id, pageSize: 100);

            var deleteRevision = metadata.FirstOrDefault(m =>
                (m.GetString(Constants.Documents.Metadata.Flags) ?? string.Empty).Contains(nameof(Raven.Server.Documents.DocumentFlags.DeleteRevision)));

            return (deleteRevision?.GetString(Constants.Documents.Metadata.ChangeVector), metadata.Count);
        }
    }
}

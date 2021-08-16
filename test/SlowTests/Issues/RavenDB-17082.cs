using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Server.Documents;
using Raven.Server.ServerWide;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_17082 : RavenTestBase
    {
        public RavenDB_17082(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task ReverrRevisionWithMoreInfo()
        {
            var names = new[]
            {
                "background-photo.jpg",
                "fileNAME_#$1^%_בעברית.txt",
                "profile.png",
            };
            DateTime last = default;
            using (var store = GetDocumentStore())
            {

                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Name1", LastName = "LastName1"}, "users/1");
                    session.CountersFor("users/1").Increment("Downloads", 100);
                    await using (var profileStream = new MemoryStream(new byte[] { 1, 2, 3 }))
                    await using (var backgroundStream = new MemoryStream(new byte[] { 10, 20, 30, 40, 50 }))
                    await using (var fileStream = new MemoryStream(new byte[] { 1, 2, 3, 4, 5 }))
                    {
                        session.Advanced.Attachments.Store("users/1", names[0], backgroundStream, "ImGgE/jPeG");
                        await session.SaveChangesAsync();
                    }
                    last = DateTime.UtcNow;
                }

                using (var session = store.OpenAsyncSession())
                {
                    session.CountersFor("users/1").Delete("Downloads");
                    session.Advanced.Attachments.Delete("users/1", "ImGgE/jPeG");
                   await session.SaveChangesAsync();
                }

                var db = await GetDocumentDatabaseInstanceFor(store);

                RevertResult result;
                using (var token = new OperationCancelToken(db.Configuration.Databases.OperationTimeout.AsTimeSpan, db.DatabaseShutdown, CancellationToken.None))
                {
                    result = (RevertResult)await db.DocumentsStorage.RevisionsStorage.RevertRevisions(last, TimeSpan.FromMinutes(60), onProgress: null,
                        token: token);
                }

                using (var session = store.OpenAsyncSession())
                {
                    var rev = await session.Advanced.Revisions.GetForAsync<User>("users/1");
                    Assert.Equal(5, rev.Count);

                    Assert.Equal("Name1", rev[0].Name);

                    var user = await session.LoadAsync<User>("users/1");
                    var metadata = session.Advanced.GetMetadataFor(user);
                    var flags = metadata.GetString(Constants.Documents.Metadata.Flags);
                    Assert.Contains(DocumentFlags.HasAttachments.ToString(), flags);
                    Assert.Contains(DocumentFlags.HasCounters.ToString(), flags);

                }
            }
        }
    }
}

using System.IO;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.ServerWide;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_19301 : ReplicationTestBase
    {
        public RavenDB_19301(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Attachments | RavenTestCategory.Replication)]
        public async Task ConflictOfAttachmentAndDocument_ManualResolution()
        {
            var options = new Options
            {
                ModifyDatabaseRecord = record => record.ConflictSolverConfig = new ConflictSolver { ResolveToLatest = false }
            };

            using (var store1 = GetDocumentStore(options))
            using (var store2 = GetDocumentStore(options))
            {
                using (var session = store1.OpenSession())
                {
                    session.Store(new User { Name = "Karmel" }, "users/1");
                    session.SaveChanges();
                }

                using (var profileStream = new MemoryStream(new byte[] { 1, 2, 3 }))
                {
                    var result = store1.Operations.Send(new PutAttachmentOperation("users/1", "foo/bar", profileStream, "image/png"));
                    Assert.Equal("foo/bar", result.Name);
                    Assert.Equal("users/1", result.DocumentId);
                    Assert.Equal("image/png", result.ContentType);
                    Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", result.Hash);
                }

                await SetupReplicationAsync(store1, store2);
                await EnsureReplicatingAsync(store1, store2);

                await SetupReplicationAsync(store2, store1);
                await EnsureReplicatingAsync(store2, store1);

                var b1 = await BreakReplication(Server.ServerStore, store1.Database);
                var b2 = await BreakReplication(Server.ServerStore, store2.Database);

                using (var backgroundStream = new MemoryStream(new byte[] { 10, 20, 30, 40, 50 }))
                {
                    var result = store2.Operations.Send(new PutAttachmentOperation("users/1", "foo/bar", backgroundStream, "image/png"));
                    Assert.Equal("foo/bar", result.Name);
                    Assert.Equal("users/1", result.DocumentId);
                    Assert.Equal("image/png", result.ContentType);
                    Assert.Equal("igkD5aEdkdAsAB/VpYm1uFlfZIP9M2LSUsD6f6RVW9U=", result.Hash);
                }

                using (var session = store1.OpenAsyncSession())
                {
                    var u = await session.LoadAsync<User>("users/1");
                    u.Age = 30;
                    await session.SaveChangesAsync();
                }

                b1.Mend();
                b2.Mend();

                Assert.Equal(2, WaitUntilHasConflict(store1, "users/1").Length);
                Assert.Equal(2, WaitUntilHasConflict(store2, "users/1").Length);

                var config = new ConflictSolver { ResolveToLatest = true };
                await UpdateConflictResolver(store1, config.ResolveByCollection, config.ResolveToLatest);
                await UpdateConflictResolver(store2, config.ResolveByCollection, config.ResolveToLatest);

                await EnsureReplicatingAsync(store1, store2);
                await EnsureReplicatingAsync(store2, store1);

                using (var session = store1.OpenAsyncSession())
                {
                    var attachment = await session.Advanced.Attachments.GetAsync("users/1", "foo/bar");
                    Assert.NotNull(attachment);
                    Assert.NotNull(attachment.Stream);
                    Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", attachment.Details.Hash);
                }

                using (var session = store2.OpenAsyncSession())
                {
                    var attachment = await session.Advanced.Attachments.GetAsync("users/1", "foo/bar");
                    Assert.NotNull(attachment);
                    Assert.NotNull(attachment.Stream);
                    Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", attachment.Details.Hash);
                }
            }
        }
    }
}

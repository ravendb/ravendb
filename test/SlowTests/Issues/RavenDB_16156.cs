using System.Threading.Tasks;
using FastTests.Server.Replication;
using FastTests.Utils;
using Raven.Client.Documents.Session;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16156 : ReplicationTestBase
    {
        public RavenDB_16156(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanRecreatedFromDeletedClusterTx()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store1.Database);

                using (var session = store1.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    await session.StoreAsync(new Person(), "karmel");
                    await session.SaveChangesAsync();
                }

                using (var session = store1.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    session.Delete("karmel");
                    await session.SaveChangesAsync();
                }

                for (int i = 0; i < 9; i++)
                {
                    using (var session = store1.OpenAsyncSession())
                    {
                        await session.StoreAsync(new Person
                        {
                            Name = i.ToString()
                        },"karmel");
                        await session.SaveChangesAsync();
                    }
                }

                await SetupReplicationAsync(store1, store2);
                await EnsureReplicatingAsync(store1,store2);
            }
        }
    }
}

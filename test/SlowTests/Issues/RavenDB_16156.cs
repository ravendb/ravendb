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

        [Fact]
        public async Task CanRecreatedFromDeletedClusterTx2()
        {
            using (var store1 = GetDocumentStore())
            {

                using (var session = store1.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    await session.StoreAsync(new Person(), "karmel");
                    await session.SaveChangesAsync();
                }

                using (var session = store1.OpenAsyncSession())
                {
                    var p = await session.LoadAsync<Person>( "karmel");
                    p.Name = "Karmel";
                    await session.SaveChangesAsync();
                }
               
                for (int i = 0; i < 9; i++)
                {
                    using (var session = store1.OpenAsyncSession())
                    {
                        await session.StoreAsync(new Person
                        {
                            Name = i.ToString()
                        },"karmel2");
                        await session.SaveChangesAsync();
                    }
                }
            }
        }

        [Fact]
        public async Task CanRecreatedFromDeletedClusterTx3()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
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
                await SetupReplicationAsync(store2, store1);
                await EnsureReplicatingAsync(store1,store2);

                using (var session = store1.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    await session.StoreAsync(new Person
                    {
                        Name = "Store2"
                    },"store2/karmel");
                    await session.SaveChangesAsync();
                }

                await EnsureReplicatingAsync(store2,store1);

                using (var session = store1.OpenAsyncSession())
                {
                    await session.StoreAsync(new Person
                    {
                        Name = "Store1"
                    },"store1/karmel");
                    await session.SaveChangesAsync();
                }

                WaitForUserToContinueTheTest(store2);
            }
        }
    }
}

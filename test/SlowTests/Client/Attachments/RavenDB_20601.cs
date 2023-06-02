using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents.Session;
using Raven.Server.Config;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.Attachments
{
    public class RavenDB_20601 : ReplicationTestBase
    {
        public RavenDB_20601(ITestOutputHelper output) : base(output)
        {
        }
        [Fact]
        public async Task ConflictOfClusterTxDocumentWithAttachment()
        {
            var co = new ServerCreationOptions
            {
                RunInMemory = false,
                CustomSettings = new Dictionary<string, string>
                {
                    [RavenConfiguration.GetKey(x => x.Replication.MaxItemsCount)] = 1.ToString()
                },
                RegisterForDisposal = false
            };
            using (var server = GetNewServer(co))
            using (var store1 = GetDocumentStore(new Options{Server = server}))
            using (var store2 = GetDocumentStore(new Options{Server = server}))
            {
                using (var session = store1.OpenSession())
                {
                    session.Advanced.SetTransactionMode(TransactionMode.ClusterWide);
                    session.Store(new User { Name = "Karmel" }, "users/1");
                    session.SaveChanges();
                }

                await SetupReplicationAsync(store1, store2);
                await EnsureReplicatingAsync(store1, store2);

                var d = await BreakReplication(server.ServerStore, store1.Database);
                using (var profileStream = new MemoryStream(new byte[] { 1, 2, 3 }))
                {
                    using (var session = store1.OpenSession())
                    {
                        session.Advanced.Attachments.Store("users/1", "foo/bar", profileStream, "image/png");
                        session.SaveChanges();
                    }
                }
                
                using (var session = store1.OpenSession())
                {
                    session.Advanced.Attachments.Delete("users/1", "foo/bar");
                    session.SaveChanges();
                }

                using (var session = store1.OpenSession())
                {
                    session.Store(new User { Name = "Karmel2" }, "users/1");
                    session.SaveChanges();
                }

                d.Mend();

                await EnsureReplicatingAsync(store1, store2);
            }
        }

        [Fact]
        public async Task ConflictOfTwoClusterTxAndAttachment()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                using (var session = store1.OpenSession())
                {
                    session.Advanced.SetTransactionMode(TransactionMode.ClusterWide);
                    session.Store(new User { Name = "Karmel" }, "users/1");
                    session.SaveChanges();
                }

                await SetupReplicationAsync(store1, store2);
                await EnsureReplicatingAsync(store1, store2);
                var d = await BreakReplication(Server.ServerStore, store1.Database);
                using (var profileStream = new MemoryStream(new byte[] { 1, 2, 3 }))
                {
                    using (var session = store1.OpenSession())
                    {
                        session.Advanced.Attachments.Store("users/1", "foo/bar", profileStream, "image/png");
                        session.SaveChanges();
                    }
                }
                
                using (var session = store2.OpenSession())
                {
                    session.Advanced.SetTransactionMode(TransactionMode.ClusterWide);
                    session.Store(new User { Name = "Karmel2" }, "users/1");
                    session.SaveChanges();
                }

                using (var session = store1.OpenSession())
                {
                    session.Advanced.Attachments.Delete("users/1", "foo/bar");
                    session.SaveChanges();
                }

                d.Mend();

                await EnsureReplicatingAsync(store1, store2);
            }
        }

        [Fact]
        public async Task ConflictOfClusterTxDocumentWithAttachment2()
        {
            var co = new ServerCreationOptions
            {
                RunInMemory = false,
                CustomSettings = new Dictionary<string, string>
                {
                    [RavenConfiguration.GetKey(x => x.Replication.MaxItemsCount)] = 1.ToString()
                },
                RegisterForDisposal = false
            };
            using (var server = GetNewServer(co))
            using (var store1 = GetDocumentStore(new Options{Server = server}))
            using (var store2 = GetDocumentStore(new Options{Server = server}))
            {
                using (var session = store1.OpenSession())
                {
                    session.Advanced.SetTransactionMode(TransactionMode.ClusterWide);
                    session.Store(new User { Name = "Karmel" }, "users/1");
                    session.SaveChanges();
                }

                await SetupReplicationAsync(store1, store2);

                await EnsureReplicatingAsync(store1, store2);
                var d = await BreakReplication(server.ServerStore, store1.Database);
                using (var profileStream = new MemoryStream(new byte[] { 1, 2, 3 }))
                {
                    using (var session = store1.OpenSession())
                    {
                        session.Advanced.Attachments.Store("users/1", "foo/bar", profileStream, "image/png");
                        session.SaveChanges();
                    }
                }
                
                using (var profileStream = new MemoryStream(new byte[] { 3, 4, 5 }))
                {
                    using (var session = store1.OpenSession())
                    {
                        session.Advanced.Attachments.Store("users/1", "foo/bar/2", profileStream, "image/png");
                        session.SaveChanges();
                    }
                }

                using (var profileStream = new MemoryStream(new byte[] { 11, 22, 33 }))
                {
                    using (var session = store1.OpenSession())
                    {
                        session.Advanced.Attachments.Store("users/1", "foo/bar", profileStream, "image/png");
                        session.SaveChanges();
                    }
                }
                
                using (var profileStream = new MemoryStream(new byte[] { 33, 44, 55 }))
                {
                    using (var session = store1.OpenSession())
                    {
                        session.Advanced.Attachments.Store("users/1", "foo/bar/2", profileStream, "image/png");
                        session.SaveChanges();
                    }
                }

                d.Mend();

                await EnsureReplicatingAsync(store1, store2);
            }
        }
    }
}

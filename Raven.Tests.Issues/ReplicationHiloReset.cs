using System;
using Raven.Database.Extensions;
using Raven.Tests.Common;
using Raven.Tests.Common.Dto.Faceted;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Issues
{
    public class ReplicationHiloReset : ReplicationBase
    {
        private readonly string DataFolder = "ReplicationHiloReset_" + Guid.NewGuid();

        [Theory]
        [InlineData("esent")]
        [InlineData("voron")]
        public void ReplicationHiloShouldnotResetAfterRestart(string storage)
        {
            long initialReplicationVersion;
            using(var server = GetNewServer(
                runInMemory: false, 
                requestedStorage:storage,
                activeBundles: "replication",
                dataDirectory:DataFolder))
            {
                using (var store = NewRemoteDocumentStore(
                    ravenDbServer: server, 
                    activeBundles:"Replication",
                    requestedStorage: storage))
                {
                    using (var session = store.OpenSession())
                    {                        
                        session.Store(new Order
                        {
                            Total = 345
                        });

                        session.SaveChanges();
                    }

                    using (var session = store.OpenSession())
                    {
                        var order = session.Load<Order>("orders/1");
                        initialReplicationVersion = session
                            .Advanced
                            .GetMetadataFor(order)["Raven-Replication-Version"]
                            .Value<long>();
                    }
                }
            }
            
            servers.ForEach(s => s.Dispose());
            servers.Clear();

            using (var server = GetNewServer(
                runInMemory: false,
                requestedStorage:storage,
                activeBundles: "replication",
                dataDirectory: DataFolder))
            {
                using (var store = NewRemoteDocumentStore(
                    ravenDbServer: server,
                    activeBundles: "Replication",
                    requestedStorage: storage))
                {
                    using (var session = store.OpenSession())
                    {
                        var order = session.Load<Order>("orders/1");
                        Assert.NotNull(order); //sanity check

                        order.Total = 567;

                        session.SaveChanges();
                    }

                    using (var session = store.OpenSession())
                    {
                        var order = session.Load<Order>("orders/1");
                        var replicationVersionAfterShutdown = session
                            .Advanced
                            .GetMetadataFor(order)["Raven-Replication-Version"]
                            .Value<long>();

                        Assert.True(replicationVersionAfterShutdown > initialReplicationVersion,string.Format("should be true -> replicationVersionAfterShutdown > initialReplicationVersion, but it is {0} > {1}",replicationVersionAfterShutdown, initialReplicationVersion));
                    }
                }
            }
        }

        [Theory]
        [InlineData("esent")]
        [InlineData("voron")]
        public void ReplicationHiloShouldnotResetAfterRestart_largeBatch(string storage)
        {
            long initialReplicationVersion;
            using(var server = GetNewServer(
                runInMemory: false, 
                requestedStorage:storage,
                activeBundles: "replication",
                dataDirectory:DataFolder))
            {
                using (var store = NewRemoteDocumentStore(
                    ravenDbServer: server, 
                    activeBundles:"Replication",
                    requestedStorage: storage))
                {
                    using (var session = store.OpenSession())
                    {
                        for (int i = 0; i < 512; i++)
                        {
                            session.Store(new Order
                            {
                                Total = 123
                            });
                        }
                        session.Store(new Order
                        {
                            Total = 345
                        });

                        session.SaveChanges();
                    }

                    using (var session = store.OpenSession())
                    {
                        var order = session.Load<Order>("orders/513");
                        initialReplicationVersion = session
                            .Advanced
                            .GetMetadataFor(order)["Raven-Replication-Version"]
                            .Value<long>();
                    }
                }
            }
            
            servers.ForEach(s => s.Dispose());
            servers.Clear();

            using (var server = GetNewServer(
                runInMemory: false,
                requestedStorage:storage,
                activeBundles: "replication",
                dataDirectory: DataFolder))
            {
                using (var store = NewRemoteDocumentStore(
                    ravenDbServer: server,
                    activeBundles: "Replication",
                    requestedStorage: storage))
                {
                    using (var session = store.OpenSession())
                    {
                        var order = session.Load<Order>("orders/513");
                        Assert.NotNull(order); //sanity check

                        order.Total = 567;

                        session.SaveChanges();
                    }

                    using (var session = store.OpenSession())
                    {
                        var order = session.Load<Order>("orders/513");
                        var replicationVersionAfterShutdown = session
                            .Advanced
                            .GetMetadataFor(order)["Raven-Replication-Version"]
                            .Value<long>();

                        Assert.True(replicationVersionAfterShutdown > initialReplicationVersion,string.Format("should be true -> replicationVersionAfterShutdown > initialReplicationVersion, but it is {0} > {1}",replicationVersionAfterShutdown, initialReplicationVersion));
                    }
                }
            }
        }


        public override void Dispose()
        {
            base.Dispose();
            IOExtensions.DeleteDirectory(DataFolder);
        }
    }
}

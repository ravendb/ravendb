using System;
using System.IO;
using System.Reflection;
using System.Threading;
using Raven.Database;
using Raven.Server;
using Xunit;
using System.Collections.Generic;
using Raven.Client.Shard;
using Rhino.Mocks;
using Raven.Client.Interface;

namespace Raven.Client.Tests
{
    public class DocumentStoreShardedServerTests : BaseTest, IDisposable
	{
        private readonly string path1;
        private readonly string path2;
        private readonly int port1;
        private readonly int port2;
        private readonly string server;

        public DocumentStoreShardedServerTests()
		{
            server = "localhost";

            port1 = 8080;
            port2 = 8081;

            path1 = GetPath("TestDb1");
            path2 = GetPath("TestDb2");

            RavenDbServer.EnsureCanListenToWhenInNonAdminContext(port1);
            RavenDbServer.EnsureCanListenToWhenInNonAdminContext(port2);
        }

        [Fact]
        public void Can_insert_into_two_servers_running_simultaneously_without_sharding()
        {
            var serversStoredUpon = new List<int>();

            using (var server1 = GetNewServer(port1, path1))
            using (var server2 = GetNewServer(port2, path2))
            {
                foreach (var port in new[] { port1, port2 })
                {
                    using (var documentStore = new DocumentStore(server, port).Initialise())
                    using (var session = documentStore.OpenSession())
                    {
                        documentStore.Stored += (storeServer, storePort, storeEntity) => serversStoredUpon.Add(storePort);

                        var entity = new Company { Name = "Company" };
                        session.Store(entity);
                        Assert.NotEqual(Guid.Empty.ToString(), entity.Id);
                    }
                }
            }

            Assert.Equal(port1, serversStoredUpon[0]);
            Assert.Equal(port2, serversStoredUpon[1]);
        }

        [Fact]
        public void Can_insert_into_two_sharded_servers()
        {
            Company company1 = new Company { Name = "Company1" };
            Company company2 = new Company { Name = "Company2" };

            var serversStoredUpon = new List<int>();

            using (var server1 = GetNewServer(port1, path1))
            using (var server2 = GetNewServer(port2, path2))
            {
                var shards = new Shards { 
                    new DocumentStore(server, port1) { Identifier="Shard1" }, 
                    new DocumentStore(server, port2) { Identifier="Shard2" } 
                };

                var shardSelection = MockRepository.GenerateStub<IShardSelectionStrategy>();
                shardSelection.Stub(x => x.SelectShardIdForNewObject(company1)).Return("Shard1");
                shardSelection.Stub(x => x.SelectShardIdForNewObject(company2)).Return("Shard2");

                using (var documentStore = new ShardedDocumentStore(shardSelection, shards))
                {
                    documentStore.Stored += (storeServer, storePort, storeEntity) => serversStoredUpon.Add(storePort);
                    documentStore.Initialise();

                    using (var session = documentStore.OpenSession())
                    {
                        var entities = new[] { company1, company2 };

                        session.StoreAll(entities);
                        Assert.NotEqual(Guid.Empty.ToString(), entities[0].Id);
                        Assert.NotEqual(Guid.Empty.ToString(), entities[1].Id);
                    }
                }
            }

            Assert.Equal(port1, serversStoredUpon[0]);
            Assert.Equal(port2, serversStoredUpon[1]);
        }

        #region IDisposable Members

        public void Dispose()
        {
            Thread.Sleep(100);

            foreach (var path in new[] { path1, path2 })
            {
                try
                {
                    Directory.Delete(path, true);
                }
                catch (Exception) { }
            }
        }

        #endregion

    }
}
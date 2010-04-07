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

namespace Raven.Client.Tests
{
    public class When_Using_Sharded_Servers : BaseTest, IDisposable
	{
        public When_Using_Sharded_Servers()
		{
            server = "localhost";

            port1 = 8080;
            port2 = 8081;

            path1 = GetPath("TestShardedDb1");
            path2 = GetPath("TestShardedDb2");

            RavenDbServer.EnsureCanListenToWhenInNonAdminContext(port1);
            RavenDbServer.EnsureCanListenToWhenInNonAdminContext(port2);

            company1 = new Company { Name = "Company1" };
            company2 = new Company { Name = "Company2" };

            server1 = GetNewServer(port1, path1);
            server2 = GetNewServer(port2, path2);
 
            shards = new Shards { 
                new DocumentStore(server, port1) { Identifier="Shard1" }, 
                new DocumentStore(server, port2) { Identifier="Shard2" } 
            };

            shardSelection = MockRepository.GenerateStub<IShardSelectionStrategy>();
            shardSelection.Stub(x => x.SelectShardIdForNewObject(company1)).Return("Shard1");
            shardSelection.Stub(x => x.SelectShardIdForNewObject(company2)).Return("Shard2");

            shardResolution = MockRepository.GenerateStub<IShardResolutionStrategy>();

            shardStrategy = MockRepository.GenerateStub<IShardStrategy>();
            shardStrategy.Stub(x => x.ShardSelectionStrategy).Return(shardSelection);
            shardStrategy.Stub(x => x.ShardResolutionStrategy).Return(shardResolution);
        }

        string path1;
        string path2;
        int port1;
        int port2;
        string server;
        RavenDbServer server1;
        RavenDbServer server2;
        Company company1;
        Company company2;
        Shards shards;
        IShardSelectionStrategy shardSelection;
        IShardResolutionStrategy shardResolution;
        IShardStrategy shardStrategy;

        [Fact]
        public void Can_insert_into_two_sharded_servers()
        {
            var serverPortsStoredUpon = new List<int>();

            using (var documentStore = new ShardedDocumentStore(shardStrategy, shards))
            {
                documentStore.Stored += (storeServer, storePort, storeEntity) => serverPortsStoredUpon.Add(storePort);
                documentStore.Initialise();

                using (var session = documentStore.OpenSession())
                {
                    session.StoreAll(new[] { company1, company2 });
                }
            }

            Assert.Equal(port1, serverPortsStoredUpon[0]);
            Assert.Equal(port2, serverPortsStoredUpon[1]);
        }

        [Fact]
        public void Can_get_entity_from_correct_sharded_server()
        {
            using (var documentStore = new ShardedDocumentStore(shardStrategy, shards).Initialise())
            using (var session = documentStore.OpenSession())
            {
                //store item that goes in 2nd shard
                session.Store(company2);

                //get it, should automagically retrieve from 2nd shard
                shardResolution.Stub(x => x.SelectShardIdsFromData(null)).IgnoreArguments().Return(new[] { "Shard2" });
                var loadedCompany = session.Load<Company>(company2.Id);

                Assert.NotNull(loadedCompany);
                Assert.Equal(company2.Name, loadedCompany.Name);
            }


        }

        #region IDisposable Members

        public void Dispose()
        {
            server1.Dispose();
            server2.Dispose();

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
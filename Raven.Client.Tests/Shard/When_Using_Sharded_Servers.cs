using System;
using System.IO;
using System.Threading;
using Raven.Client.Document;
using Raven.Client.Shard.ShardStrategy;
using Raven.Client.Shard.ShardStrategy.ShardAccess;
using Raven.Client.Shard.ShardStrategy.ShardResolution;
using Raven.Client.Shard.ShardStrategy.ShardSelection;
using Raven.Client.Tests.Document;
using Raven.Server;
using Xunit;
using System.Collections.Generic;
using Raven.Client.Shard;
using Rhino.Mocks;
using System.Linq;

namespace Raven.Client.Tests.Shard
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
                new DocumentStore { Identifier="Shard1", Url = "http://" + server +":"+port1}, 
                new DocumentStore { Identifier="Shard2", Url = "http://" + server +":"+port2} 
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
            var serverPortsStoredUpon = new List<string>();

            using (var documentStore = new ShardedDocumentStore(shardStrategy, shards))
            {
                documentStore.Stored += (storeServer, storeEntity) => serverPortsStoredUpon.Add(storeServer);
                documentStore.Initialise();

                using (var session = documentStore.OpenSession())
                {
                	session.Store(company1);
					session.Store(company2);
					session.SaveChanges();
                }
            }

            Assert.Contains(port1.ToString(), serverPortsStoredUpon[0]);
            Assert.Contains(port2.ToString(), serverPortsStoredUpon[1]);
        }

        [Fact]
        public void Can_get_single_entity_from_correct_sharded_server()
        {
            using (var documentStore = new ShardedDocumentStore(shardStrategy, shards).Initialise())
            using (var session = documentStore.OpenSession())
            {
                //store item that goes in 2nd shard
                session.Store(company2);
				session.SaveChanges();

                //get it, should automagically retrieve from 2nd shard
                shardResolution.Stub(x => x.SelectShardIdsFromData(null)).IgnoreArguments().Return(new[] { "Shard2" });
                var loadedCompany = session.Load<Company>(company2.Id);

                Assert.NotNull(loadedCompany);
                Assert.Equal(company2.Name, loadedCompany.Name);
            }
        }

        [Fact]
        public void Can_get_single_entity_from_correct_sharded_server_when_location_is_unknown()
        {
			shardStrategy.Stub(x => x.ShardAccessStrategy).Return(new SequentialShardAccessStrategy());
			
			using (var documentStore = new ShardedDocumentStore(shardStrategy, shards).Initialise())
            using (var session = documentStore.OpenSession())
            {
                //store item that goes in 2nd shard
                session.Store(company2);

				session.SaveChanges();

                //get it, should try all shards and find it
                shardResolution.Stub(x => x.SelectShardIdsFromData(null)).IgnoreArguments().Return(null);
                var loadedCompany = session.Load<Company>(company2.Id);

                Assert.NotNull(loadedCompany);
                Assert.Equal(company2.Name, loadedCompany.Name);
            }
        }

        [Fact]
        public void Can_get_all_sharded_entities()
        {
			//get them in simple single threaded sequence for this test
			shardStrategy.Stub(x => x.ShardAccessStrategy).Return(new SequentialShardAccessStrategy());

            using (var documentStore = new ShardedDocumentStore(shardStrategy, shards).Initialise())
            using (var session = documentStore.OpenSession())
            {
                //store 2 items in 2 shards
				session.Store(company1);
				session.Store(company2);

				session.SaveChanges();

             
                //get all, should automagically retrieve from each shard
                var allCompanies = session.Query<Company>()
					.WaitForNonStaleResults()
					.ToArray();

                Assert.NotNull(allCompanies);
                Assert.Equal(company1.Name, allCompanies[0].Name);
                Assert.Equal(company2.Name, allCompanies[1].Name);
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
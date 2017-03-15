using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Server;
using Raven.Client.Server.Operations;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Rachis
{
    
    public class Cluster: ClusterTestBase
    {
        [Fact]
        public async Task test()
        {
            NoTimeouts();
            //var leader = await CreateRaftClusterAndGetLeader(3);
            var leader = await CreateRaftClusterAndGetLeader(2);
            CreateDatabaseResult databaseResult;
            using (var store = new DocumentStore()
            {
                Url = leader.WebUrls[0],
                DefaultDatabase = "test"
            }.Initialize())
            {
                var doc = MultiDatabase.CreateDatabaseDocument("test");
                databaseResult = store.Admin.Server.Send(new CreateDatabaseOperation(doc));
            }
    
            foreach (var server in Servers.Except(new [] {leader}))
            {
                await server.ServerStore.Cluster.WaitForIndexNotification(databaseResult.ETag ?? 0);

                using (var store = new DocumentStore() { Url = server.WebUrls[0] }.Initialize())
                using(var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Person {Name = "blah"});
                    await session.SaveChangesAsync();
                }
            }
        }

        public class Person
        {
            public string Name { get; set; }
        }
    }
}

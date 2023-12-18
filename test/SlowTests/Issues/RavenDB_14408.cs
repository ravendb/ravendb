using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Server;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14408 : ClusterTestBase
    {
        public RavenDB_14408(ITestOutputHelper output) : base(output)
        {
        }
        
        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task HandleServerDownWhileReadingResponse()
        {
            var cluster = await CreateRaftCluster(3, leaderIndex: 2, watcherCluster: true);
            var members = new List<string>()
            {
                cluster.Nodes[0].ServerStore.NodeTag,
                cluster.Nodes[1].ServerStore.NodeTag
            };
            using (var store = GetDocumentStore(new Options()
            {
                Server = cluster.Nodes[1],
                ModifyDatabaseRecord = record =>
                {
                    record.Topology = new DatabaseTopology();
                    record.Topology.Members = members;
                    record.Topology.PriorityOrder = members;
                },
            }))
            {
                //create query - will immediately fill the first part of the response in the buffer
                using (var session = store.OpenSession())
                {
                    // need to be enough data to fill more than one buffer
                    for (int i = 0; i < 300; i++)
                    {
                        session.Store(new User(), $"Users/{i}");
                    }
                    session.SaveChanges();

                    // bring down server while there are still things to flush
                    var server = cluster.Nodes[0];
                    
                    server.ServerStore.ForTestingPurposesOnly().AdjustResult = (results) =>
                    {
                        var r = results.Results.Single(r => r.Id == "Users/200");
                        r.Data = null; // throw NRE and fail when writing second batch of docs to response stream
                    };
                    
                    var q = session.Advanced.RawQuery<User>("from Users");
                    var syncedRes = q.ToList();
                    Assert.Equal(300, syncedRes.Count());
                }
            }
        }
    }
}

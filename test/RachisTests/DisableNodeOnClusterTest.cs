using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Server.Config;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace RachisTests
{
    public class DisableNodeOnClusterTest : ReplicationTestBase
    {
        public DisableNodeOnClusterTest(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task BackToFirstNodeAfterRevive()
        {
            // we don't want to move the node to rehab, since it should be restored to the top of the list.
            var settings = new Dictionary<string, string>()
            {
                [RavenConfiguration.GetKey(x => x.Cluster.StabilizationTime)] = "1",
                [RavenConfiguration.GetKey(x => x.Cluster.RotatePreferredNodeGraceTime)] = "15"
            };

            var (_, leader) = await CreateRaftCluster(3, shouldRunInMemory: false, customSettings: settings);
            using (var store = GetDocumentStore(new Options
                   {
                       Server = leader,
                       ReplicationFactor = 3
                   }))
            {
                using (var session = store.OpenSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(replicas: 2, timeout: TimeSpan.FromSeconds(30));
                    session.Store(new User
                    {
                        Name = "Idan"
                    });
                    session.SaveChanges();
                }

                var re = store.GetRequestExecutor();
                var firstNodeUrl = re.Url;
                var firstNode = Servers.Single(s => s.WebUrl == firstNodeUrl);

                var result = await DisposeServerAndWaitForFinishOfDisposalAsync(firstNode);

                // check that replication works.
                using (var session = store.OpenSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(replicas: 1, timeout: TimeSpan.FromSeconds(30));
                    session.Store(new User
                    {
                        Name = "Karmel"
                    }, "users/1");
                    session.SaveChanges();
                }

                Assert.NotEqual(re.Url, firstNodeUrl);
                settings[RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = firstNodeUrl;
                Servers.Add(GetNewServer(new ServerCreationOptions
                {
                    CustomSettings = settings,
                    RunInMemory = false,
                    DeletePrevious = false,
                    DataDirectory = result.DataDirectory
                }));
                await re.CheckNodeStatusNow(result.NodeTag);
                Assert.True(WaitForValue(() => firstNodeUrl == re.Url, true));
            }
        }
    }
}

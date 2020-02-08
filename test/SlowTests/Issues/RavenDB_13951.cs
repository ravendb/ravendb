using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Http;
using Raven.Client.ServerWide.Commands.Cluster;
using Raven.Server.Config;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_13951 : ReplicationTestBase
    {
        public RavenDB_13951(ITestOutputHelper output) : base(output)
        {
        }

        [Fact(Skip = "Test changes number of utilized cores and cannot be run in whole test suite")]
        public async Task UtilizedCoresShouldNotChangeAfterRestart()
        {
            var server = GetNewServer(new ServerCreationOptions
            {
                RunInMemory = false
            });

            using var store = GetDocumentStore(new Options
            {
                Server = server,
                Path = Path.Combine(server.Configuration.Core.DataDirectory.FullPath, "UtilizedCoresShouldNotChangeAfterRestart")
            });

            await server.ServerStore.LicenseManager.ChangeLicenseLimits(server.ServerStore.NodeTag, 1, Guid.NewGuid().ToString());
            var license = server.ServerStore.LoadLicenseLimits();
            Assert.True(license.NodeLicenseDetails.TryGetValue(server.ServerStore.NodeTag, out var detailsPerNode));
            Assert.True(detailsPerNode.UtilizedCores == 1, "detailsPerNode.UtilizedCores == 1");

            // Taking down server
            var serverPath = server.Configuration.Core.DataDirectory.FullPath;
            var nodePath = serverPath.Split('/').Last();
            var url = server.WebUrl;
            await DisposeServerAndWaitForFinishOfDisposalAsync(server);
            var settings = new Dictionary<string, string>
            {
                { RavenConfiguration.GetKey(x => x.Core.ServerUrls), url }
            };

            // Bring server up
            server = GetNewServer(new ServerCreationOptions { RunInMemory = false, DeletePrevious = false, DataDirectory = nodePath, CustomSettings = settings });

            license = server.ServerStore.LoadLicenseLimits();
            Assert.True(license.NodeLicenseDetails.TryGetValue(server.ServerStore.NodeTag, out detailsPerNode));
            Assert.True(detailsPerNode.UtilizedCores == 1, "detailsPerNode.UtilizedCores == 1");
        }

        [Fact(Skip = "Test changes number of utilized cores and cannot be run in whole test suite")]
        public async Task DemotePromoteShouldNotChangeNodesTheUtilizedCores()
        {
            var reasonableTime = Debugger.IsAttached ? 5000 : 3000;
            var leader = await CreateRaftClusterAndGetLeader(3);

            using var store = GetDocumentStore(new Options
            {
                CreateDatabase = true,
                ReplicationFactor = 3,
                Server = leader
            });

            var tags = Servers.Where(x => x.ServerStore.NodeTag != leader.ServerStore.NodeTag).Select(x => x.ServerStore.NodeTag).ToList();
            foreach (var tag in tags)
            {
                await ActionWithLeader(l => l.ServerStore.LicenseManager.ChangeLicenseLimits(tag, 1, Guid.NewGuid().ToString()));
                var license = leader.ServerStore.LoadLicenseLimits();
                Assert.True(license.NodeLicenseDetails.TryGetValue(tag, out var detailsPerNode), "license.NodeLicenseDetails.TryGetValue(tag, out var detailsPerNode)");
                Assert.True(detailsPerNode.UtilizedCores == 1, "detailsPerNode.UtilizedCores == 1");
            }

            foreach (var tag in tags)
            {
                var re = store.GetRequestExecutor(store.Database);
                using (re.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    re.Execute(new DemoteClusterNodeCommand(tag), context);
                    await Task.Delay(reasonableTime);

                    using (leader.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                    {
                        ctx.OpenReadTransaction();
                        var topology = leader.ServerStore.GetClusterTopology(ctx);
                        Assert.True(topology.Watchers.ContainsKey(tag), "topology.Watchers.ContainsKey(tag)");
                    }

                    var license = leader.ServerStore.LoadLicenseLimits();
                    Assert.True(license.NodeLicenseDetails.TryGetValue(tag, out var detailsPerNode), "license.NodeLicenseDetails.TryGetValue(tag, out var detailsPerNode)");
                    Assert.True(detailsPerNode.UtilizedCores == 1, "detailsPerNode.UtilizedCores == 1");

                    re.Execute(new PromoteClusterNodeCommand(tag), context);
                    await Task.Delay(reasonableTime);

                    using (leader.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                    {
                        ctx.OpenReadTransaction();
                        var topology = leader.ServerStore.GetClusterTopology(ctx);
                        Assert.True(topology.Watchers.ContainsKey(tag) == false, "topology.Watchers.ContainsKey(tag) == false");
                    }

                    license = leader.ServerStore.LoadLicenseLimits();
                    Assert.True(license.NodeLicenseDetails.TryGetValue(tag, out detailsPerNode), "license.NodeLicenseDetails.TryGetValue(tag, out detailsPerNode)");
                    Assert.True(detailsPerNode.UtilizedCores == 1, "detailsPerNode.UtilizedCores == 1");
                }
            }
        }
    }
}

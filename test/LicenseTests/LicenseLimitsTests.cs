using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.ServerWide.Commands.Cluster;
using Raven.Server.Config;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Utils;
using Xunit;
using Xunit.Abstractions;

namespace LicenseTests
{
    public class LicenseLimitsTests : ReplicationTestBase
    {
        static LicenseLimitsTests()
        {
            IgnoreProcessorAffinityChanges(ignore: false);
        }

        public LicenseLimitsTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task WillUtilizeAllAvailableCores()
        {
            var server = GetNewServer(new ServerCreationOptions
            {
                RunInMemory = false
            });

            await server.ServerStore.EnsureNotPassiveAsync();

            await server.ServerStore.LicenseManager.ChangeLicenseLimits(server.ServerStore.NodeTag, 1, Guid.NewGuid().ToString());
            var licenseLimits = server.ServerStore.LoadLicenseLimits();
            Assert.True(licenseLimits.NodeLicenseDetails.TryGetValue(server.ServerStore.NodeTag, out var detailsPerNode));
            Assert.True(detailsPerNode.UtilizedCores == 1, "detailsPerNode.UtilizedCores == 1");

            // Taking down server
            var result = await DisposeServerAndWaitForFinishOfDisposalAsync(server);
            var settings = new Dictionary<string, string>
            {
                {RavenConfiguration.GetKey(x => x.Core.ServerUrls), result.Url}
            };

            // Bring server up
            server = GetNewServer(
                new ServerCreationOptions { RunInMemory = false, DeletePrevious = false, DataDirectory = result.DataDirectory, CustomSettings = settings });

            licenseLimits = server.ServerStore.LoadLicenseLimits();
            Assert.True(licenseLimits.NodeLicenseDetails.TryGetValue(server.ServerStore.NodeTag, out detailsPerNode));
            Assert.True(detailsPerNode.UtilizedCores == 1, "detailsPerNode.UtilizedCores == 1");

            await server.ServerStore.LicenseManager.ChangeLicenseLimits(server.ServerStore.NodeTag, null, Guid.NewGuid().ToString());
            licenseLimits = server.ServerStore.LoadLicenseLimits();
            Assert.True(licenseLimits.NodeLicenseDetails.TryGetValue(server.ServerStore.NodeTag, out detailsPerNode));
            Assert.True(detailsPerNode.UtilizedCores == ProcessorInfo.ProcessorCount, $"detailsPerNode.UtilizedCores == {ProcessorInfo.ProcessorCount}");
        }

        [Fact]
        public async Task WillUtilizeAllAvailableCoresInACluster()
        {
            DoNotReuseServer();

            var (servers, leader) = await CreateRaftCluster(5);
            await leader.ServerStore.EnsureNotPassiveAsync();

            foreach (var server in servers)
            {
                await server.ServerStore.LicenseManager.ChangeLicenseLimits(server.ServerStore.NodeTag, 1, Guid.NewGuid().ToString());

                var licenseLimits = server.ServerStore.LoadLicenseLimits();
                Assert.True(licenseLimits.NodeLicenseDetails.TryGetValue(server.ServerStore.NodeTag, out var detailsPerNode),
                    "license.NodeLicenseDetails.TryGetValue(tag, out var detailsPerNode)");
                Assert.Equal(1, detailsPerNode.UtilizedCores);
            }

            var seenNodeTags = new HashSet<string>();
            foreach (var server in servers)
            {
                await server.ServerStore.LicenseManager.ChangeLicenseLimits(server.ServerStore.NodeTag, null, Guid.NewGuid().ToString());
                seenNodeTags.Add(server.ServerStore.NodeTag);

                var licenseLimits = server.ServerStore.LoadLicenseLimits();
                Assert.True(licenseLimits.NodeLicenseDetails.TryGetValue(server.ServerStore.NodeTag, out var detailsPerNode),
                    "license.NodeLicenseDetails.TryGetValue(tag, out var detailsPerNode)");
                Assert.Equal(ProcessorInfo.ProcessorCount, detailsPerNode.UtilizedCores);

                var notChangedServers = servers.Select(x => x.ServerStore).Where(x => seenNodeTags.Contains(x.NodeTag) == false);
                foreach (var notChangedServer in notChangedServers)
                {
                    licenseLimits = notChangedServer.LoadLicenseLimits();
                    Assert.True(licenseLimits.NodeLicenseDetails.TryGetValue(notChangedServer.NodeTag, out detailsPerNode),
                        "license.NodeLicenseDetails.TryGetValue(tag, out var detailsPerNode)");
                    Assert.Equal(1, detailsPerNode.UtilizedCores);
                }
            }
        }

        [Fact]
        public async Task UtilizedCoresShouldNotChangeAfterRestart()
        {
            var server = GetNewServer(new ServerCreationOptions
            {
                RunInMemory = false
            });

            using (GetDocumentStore(new Options
            {
                Server = server,
                Path = Path.Combine(server.Configuration.Core.DataDirectory.FullPath, "UtilizedCoresShouldNotChangeAfterRestart")
            }))
            {
                await server.ServerStore.LicenseManager.ChangeLicenseLimits(server.ServerStore.NodeTag, 1, Guid.NewGuid().ToString());
                var license = server.ServerStore.LoadLicenseLimits();
                Assert.True(license.NodeLicenseDetails.TryGetValue(server.ServerStore.NodeTag, out var detailsPerNode));
                Assert.True(detailsPerNode.UtilizedCores == 1, "detailsPerNode.UtilizedCores == 1");

                // Taking down server
                var result = await DisposeServerAndWaitForFinishOfDisposalAsync(server);
                var settings = new Dictionary<string, string>
                {
                    { RavenConfiguration.GetKey(x => x.Core.ServerUrls), result.Url }
                };

                // Bring server up
                server = GetNewServer(new ServerCreationOptions { RunInMemory = false, DeletePrevious = false, DataDirectory = result.DataDirectory, CustomSettings = settings });

                license = server.ServerStore.LoadLicenseLimits();
                Assert.True(license.NodeLicenseDetails.TryGetValue(server.ServerStore.NodeTag, out detailsPerNode));
                Assert.True(detailsPerNode.UtilizedCores == 1, "detailsPerNode.UtilizedCores == 1");
            }
        }

        [Fact]
        public async Task DemotePromoteShouldNotChangeTheUtilizedCores()
        {
            DoNotReuseServer();

            var reasonableTime = Debugger.IsAttached ? 5000 : 3000;
            var (servers, leader) = await CreateRaftCluster(3);

            using (var store = GetDocumentStore(new Options
            {
                CreateDatabase = true,
                ReplicationFactor = 3,
                Server = leader
            }))
            {
                foreach (var server in servers)
                {
                    await server.ServerStore.LicenseManager.ChangeLicenseLimits(server.ServerStore.NodeTag, 1, Guid.NewGuid().ToString());

                    var license = server.ServerStore.LoadLicenseLimits();
                    Assert.True(license.NodeLicenseDetails.TryGetValue(server.ServerStore.NodeTag, out var detailsPerNode), "license.NodeLicenseDetails.TryGetValue(tag, out var detailsPerNode)");
                    Assert.True(detailsPerNode.UtilizedCores == 1, "detailsPerNode.UtilizedCores == 1");
                }

                foreach (var tag in servers.Select(x => x.ServerStore.NodeTag).Where(x => x != leader.ServerStore.NodeTag))
                {
                    var re = store.GetRequestExecutor(store.Database);
                    using (re.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                    {
                        await re.ExecuteAsync(new DemoteClusterNodeCommand(tag), context);
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

                        await re.ExecuteAsync(new PromoteClusterNodeCommand(tag), context);
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
}

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Commands.Cluster;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Commercial;
using Raven.Server.Config;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Sparrow.Utils;
using Tests.Infrastructure;
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
                    Assert.True(license.NodeLicenseDetails.TryGetValue(server.ServerStore.NodeTag, out var detailsPerNode), $"license.NodeLicenseDetails.TryGetValue(tag:{server.ServerStore.NodeTag}, out var detailsPerNode:{detailsPerNode})");
                    Assert.True(detailsPerNode.UtilizedCores == 1, $"detailsPerNode.UtilizedCores:{detailsPerNode.UtilizedCores} == 1");
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
                            Assert.True(topology.Watchers.ContainsKey(tag), $"topology.Watchers.ContainsKey(tag:{tag})");
                        }

                        var license = leader.ServerStore.LoadLicenseLimits();
                        Assert.True(license.NodeLicenseDetails.TryGetValue(tag, out var detailsPerNode), $"license.NodeLicenseDetails.TryGetValue(tag:{tag}, out var detailsPerNode:{detailsPerNode})");
                        Assert.True(detailsPerNode.UtilizedCores == 1, $"detailsPerNode.UtilizedCores:{detailsPerNode.UtilizedCores} == 1");

                        await re.ExecuteAsync(new PromoteClusterNodeCommand(tag), context);
                        await Task.Delay(reasonableTime);

                        using (leader.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                        {
                            ctx.OpenReadTransaction();
                            var topology = leader.ServerStore.GetClusterTopology(ctx);
                            Assert.True(topology.Watchers.ContainsKey(tag) == false, $"topology.Watchers.ContainsKey(tag:{tag}) == false");
                        }

                        license = leader.ServerStore.LoadLicenseLimits();
                        Assert.True(license.NodeLicenseDetails.TryGetValue(tag, out detailsPerNode), $"license.NodeLicenseDetails.TryGetValue(tag:{tag}, out detailsPerNode:{detailsPerNode})");
                        Assert.True(detailsPerNode.UtilizedCores == 1, $"detailsPerNode.UtilizedCores:{detailsPerNode.UtilizedCores} == 1");
                    }
                }
            }
        }

        [RavenTheory(RavenTestCategory.Licensing)]
        [InlineData(LicenseType.None, SearchEngineType.Lucene, SearchEngineType.Lucene)]
        [InlineData(LicenseType.None, SearchEngineType.Corax, SearchEngineType.Corax)]
        [InlineData(LicenseType.None, SearchEngineType.None, SearchEngineType.Corax)]
        [InlineData(LicenseType.Community, SearchEngineType.Lucene, SearchEngineType.Lucene)]
        [InlineData(LicenseType.Community, SearchEngineType.Corax, SearchEngineType.Corax)]
        [InlineData(LicenseType.Community, SearchEngineType.None, SearchEngineType.Corax)]
        [InlineData(LicenseType.Developer, SearchEngineType.Lucene, SearchEngineType.Lucene)]
        [InlineData(LicenseType.Developer, SearchEngineType.Corax, SearchEngineType.Corax)]
        [InlineData(LicenseType.Developer, SearchEngineType.None, SearchEngineType.Corax)]
        [InlineData(LicenseType.Professional, SearchEngineType.Lucene, SearchEngineType.Lucene)]
        [InlineData(LicenseType.Professional, SearchEngineType.Corax, SearchEngineType.Corax)]
        [InlineData(LicenseType.Professional, SearchEngineType.None, SearchEngineType.Lucene)]
        [InlineData(LicenseType.Enterprise, SearchEngineType.Lucene, SearchEngineType.Lucene)]
        [InlineData(LicenseType.Enterprise, SearchEngineType.Corax, SearchEngineType.Corax)]
        [InlineData(LicenseType.Enterprise, SearchEngineType.None, SearchEngineType.Lucene)]
        [InlineData(LicenseType.Essential, SearchEngineType.Lucene, SearchEngineType.Lucene)]
        [InlineData(LicenseType.Essential, SearchEngineType.Corax, SearchEngineType.Corax)]
        [InlineData(LicenseType.Essential, SearchEngineType.None, SearchEngineType.Lucene)]
        public async Task UsingCorrectIndexingEngine(LicenseType licenseType, SearchEngineType serverIndexingEngineType, SearchEngineType expectedSearchEngine)
        {
            DoNotReuseServer();

            var customSettings = new Dictionary<string, string>();
            
            if (serverIndexingEngineType == SearchEngineType.None)
            {
                customSettings[RavenConfiguration.GetKey(x => x.Indexing.AutoIndexingEngineType)] = null;
                customSettings[RavenConfiguration.GetKey(x => x.Indexing.StaticIndexingEngineType)] = null;
            }
            else
            {
                customSettings[RavenConfiguration.GetKey(x => x.Indexing.AutoIndexingEngineType)] = serverIndexingEngineType.ToString();
                customSettings[RavenConfiguration.GetKey(x => x.Indexing.StaticIndexingEngineType)] = serverIndexingEngineType.ToString();
            }

            var server = GetNewServer(new ServerCreationOptions
            {
                CustomSettings = customSettings
            });

            using (var store = GetDocumentStore(new Options
            {
                CreateDatabase = false,
                Server = server
            }))
            {
                server.Configuration.UpdateLicenseType(licenseType);

                var dbRecord = new DatabaseRecord(store.Database);
                await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(dbRecord));

                var database = await GetDatabase(server, store.Database);
                Assert.Equal(expectedSearchEngine, database.Configuration.Indexing.AutoIndexingEngineType);
                Assert.Equal(expectedSearchEngine, database.Configuration.Indexing.StaticIndexingEngineType);

                var indexToDeploy = new Index(name: "test", engineType: null);
                await indexToDeploy.ExecuteAsync(store);

                var index = database.IndexStore.GetIndex(indexToDeploy.IndexName);
                Assert.Equal(expectedSearchEngine, index.Configuration.AutoIndexingEngineType);
                Assert.Equal(expectedSearchEngine, index.Configuration.StaticIndexingEngineType);
                Assert.Equal(expectedSearchEngine, index.GetStats().SearchEngineType);

                indexToDeploy = new Index(name: "corax", engineType: SearchEngineType.Corax);
                await indexToDeploy.ExecuteAsync(store);
                index = database.IndexStore.GetIndex(indexToDeploy.IndexName);
                Assert.Equal(SearchEngineType.Corax, index.GetStats().SearchEngineType);

                indexToDeploy = new Index(name: "lucene", engineType: SearchEngineType.Lucene);
                await indexToDeploy.ExecuteAsync(store);
                index = database.IndexStore.GetIndex(indexToDeploy.IndexName);
                Assert.Equal(SearchEngineType.Lucene, index.GetStats().SearchEngineType);

                using (var session = store.OpenAsyncSession())
                {
                    await session.Query<User>().Where(x => x.Name == "Grisha").ToListAsync();
                }

                index = database.IndexStore.GetIndex("Auto/Users/ByName");
                Assert.Equal(expectedSearchEngine, index.Configuration.AutoIndexingEngineType);
                Assert.Equal(expectedSearchEngine, index.Configuration.StaticIndexingEngineType);
                Assert.Equal(expectedSearchEngine, index.GetStats().SearchEngineType);
            }
        }

        private class Index : AbstractIndexCreationTask<User>
        {
            private string _name;

            public override string IndexName => _name;

            public Index(string name, SearchEngineType? engineType)
            {
                _name = name;

                Map = docs =>
                    from doc in docs
                    select new
                    {
                        Name = doc.Name
                    };

                SearchEngineType = engineType;
            }
        }
    }
}

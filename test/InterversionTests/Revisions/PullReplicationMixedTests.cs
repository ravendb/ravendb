using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using static InterversionTests.Revisions.RevisionsInterversionHelpers;

namespace InterversionTests.Revisions
{
    public class PullReplicationMixedTests : InterversionTestBase
    {
        public PullReplicationMixedTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenMultiplatformFact(RavenTestCategory.Revisions | RavenTestCategory.Replication | RavenTestCategory.Interversion, RavenPlatform.Windows | RavenPlatform.Linux)]
        public async Task PullReplication_NewHubToOldSink_RevisionsConverge()
        {
            await RunPullScenario(hubIsNew: true);
        }

        [RavenMultiplatformFact(RavenTestCategory.Revisions | RavenTestCategory.Replication | RavenTestCategory.Interversion, RavenPlatform.Windows | RavenPlatform.Linux)]
        public async Task PullReplication_OldHubToNewSink_RevisionsConverge()
        {
            await RunPullScenario(hubIsNew: false);
        }

        private async Task RunPullScenario(bool hubIsNew)
        {
            var customSettings = new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Core.RunInMemory)] = "false",
                [RavenConfiguration.GetKey(x => x.Licensing.EulaAccepted)] = "true",
            };

            var oldNode = await GetServerAsync(Versions.PrePRv62, customSettings: customSettings);

            using var newStore = GetDocumentStore(new Options
            {
                Path = NewDataPath(suffix: "new"),
                RunInMemory = false
            });

            var oldDb = GetDatabaseName() + "-old";
            using var oldStore = new DocumentStore
            {
                Urls = new[] { oldNode.Url },
                Database = oldDb
            };
            oldStore.Initialize();
            await oldStore.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(oldDb)
            {
                Settings = { [RavenConfiguration.GetKey(x => x.Core.RunInMemory)] = "false" }
            }));

            await ConfigureRevisionsAsync(newStore);
            await ConfigureRevisionsAsync(oldStore);

            var hub = hubIsNew ? newStore : oldStore;
            var sink = hubIsNew ? oldStore : newStore;

            const string hubDefName = "rev-pull";

            await hub.Maintenance.ForDatabase(hub.Database).SendAsync(new PutPullReplicationAsHubOperation(hubDefName));

            await sink.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
            {
                Name = $"cs-{hub.Database}",
                Database = hub.Database,
                TopologyDiscoveryUrls = new[] { hub.Urls[0] }
            }));

            await sink.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(new PullReplicationAsSink
            {
                ConnectionStringName = $"cs-{hub.Database}",
                HubName = hubDefName,
                Mode = PullReplicationMode.HubToSink
            }));

            const string docId = "users/pull";
            using (var session = hub.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "v0" }, docId);
                await session.SaveChangesAsync();
            }
            var v0Cv = await GetLatestRevisionCvAsync(hub, docId);

            using (var session = hub.OpenAsyncSession())
            {
                var u = await session.LoadAsync<User>(docId);
                u.Name = "v1";
                await session.SaveChangesAsync();
            }
            var v1Cv = await GetLatestRevisionCvAsync(hub, docId);

            await WaitForExactRevisionCvsAsync(sink, docId, new[] { v1Cv, v0Cv },
                label: $"hub={(hubIsNew ? "new" : "old")} sink revisions");
        }
    }
}

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
    public class ExternalReplicationMixedTests : InterversionTestBase
    {
        public ExternalReplicationMixedTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenMultiplatformFact(RavenTestCategory.Revisions | RavenTestCategory.Replication | RavenTestCategory.Interversion, RavenPlatform.Windows | RavenPlatform.Linux)]
        public async Task ExternalReplication_NewToOld_RevisionsAndTombstoneConverge()
        {
            await RunDirectionalTest(sourceIsNew: true);
        }

        [RavenMultiplatformFact(RavenTestCategory.Revisions | RavenTestCategory.Replication | RavenTestCategory.Interversion, RavenPlatform.Windows | RavenPlatform.Linux)]
        public async Task ExternalReplication_OldToNew_RevisionsAndTombstoneConverge()
        {
            await RunDirectionalTest(sourceIsNew: false);
        }

        private async Task RunDirectionalTest(bool sourceIsNew)
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

            // Manually built to avoid GetDocumentStoreAsync's RunInMemory default.
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

            var source = sourceIsNew ? newStore : oldStore;
            var destination = sourceIsNew ? oldStore : newStore;

            await SetupExternalReplicationAsync(source, destination);

            var docId = "users/repl";

            using (var session = source.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "v0" }, docId);
                await session.SaveChangesAsync();
            }
            var v0Cv = await GetLatestRevisionCvAsync(source, docId);

            using (var session = source.OpenAsyncSession())
            {
                var u = await session.LoadAsync<User>(docId);
                u.Name = "v1";
                await session.SaveChangesAsync();
            }
            var v1Cv = await GetLatestRevisionCvAsync(source, docId);

            await WaitForExactRevisionCvsAsync(destination, docId, new[] { v1Cv, v0Cv },
                label: $"source={(sourceIsNew ? "new" : "old")} dest revisions");

            using (var session = source.OpenAsyncSession())
            {
                session.Delete(docId);
                await session.SaveChangesAsync();
            }

            await WaitForExactRevisionCountAsync(destination, docId, expected: 3,
                label: $"source={(sourceIsNew ? "new" : "old")} dest post-delete count");

            using var s = destination.OpenAsyncSession();
            var md = await s.Advanced.Revisions.GetMetadataForAsync(docId, pageSize: 1);
            var flags = md[0].GetString("@flags") ?? "";
            Assert.Contains("DeleteRevision", flags);
        }
    }
}

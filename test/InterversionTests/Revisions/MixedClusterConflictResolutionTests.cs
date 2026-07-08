using System;
using System.Collections.Generic;
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
    // Bidirectional external replication causes a conflict; ResolveToLatest converges to 1 Resolved + 2 Conflicted per peer.
    public class MixedClusterConflictResolutionTests : InterversionTestBase
    {
        public MixedClusterConflictResolutionTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenMultiplatformFact(RavenTestCategory.Revisions | RavenTestCategory.Replication | RavenTestCategory.Interversion, RavenPlatform.Windows | RavenPlatform.Linux)]
        public async Task CrossVersion_Conflict_ResolvesToLatest_WithRevisionsOnBothSides()
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

            const string docId = "users/conflict";

            using (var session = newStore.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "new-version" }, docId);
                await session.SaveChangesAsync();
            }
            using (var session = oldStore.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "old-version" }, docId);
                await session.SaveChangesAsync();
            }

            await SetupExternalReplicationAsync(newStore, oldStore);
            await SetupExternalReplicationAsync(oldStore, newStore);

            await newStore.Maintenance.Server.SendAsync(
                new ModifyConflictSolverOperation(newStore.Database, null, resolveToLatest: true));

            await WaitForExactRevisionCountAsync(newStore, docId, expected: 3, label: "conflict on new peer");
            await WaitForExactRevisionCountAsync(oldStore, docId, expected: 3, label: "conflict on old peer");

            // Newest-first metadata layout: [0]=Resolved, [1,2]=Conflicted.
            using (var session = newStore.OpenAsyncSession())
            {
                var md = await session.Advanced.Revisions.GetMetadataForAsync(docId, pageSize: 10);
                AssertFlags(md[0], expected: "Resolved", label: "new peer top revision");
                AssertFlags(md[1], expected: "Conflicted", label: "new peer 2nd revision");
                AssertFlags(md[2], expected: "Conflicted", label: "new peer 3rd revision");
            }

            using (var session = oldStore.OpenAsyncSession())
            {
                var md = await session.Advanced.Revisions.GetMetadataForAsync(docId, pageSize: 10);
                AssertFlags(md[0], expected: "Resolved", label: "old peer top revision");
                AssertFlags(md[1], expected: "Conflicted", label: "old peer 2nd revision");
                AssertFlags(md[2], expected: "Conflicted", label: "old peer 3rd revision");
            }
        }

        private static void AssertFlags(Raven.Client.Documents.Session.IMetadataDictionary metadata, string expected, string label)
        {
            var flags = metadata.GetString("@flags") ?? "";
            Assert.Contains(expected, flags);
        }
    }
}

using System;
using System.IO;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide.Operations;
using SlowTests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding.Issues
{
    public class RavenDB_20110 : RavenTestBase
    {
        public RavenDB_20110(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Sharding)]
        public async Task ShouldSkipUnsupportedFeaturesInShardingOnImport()
        {
            var file = GetTempFileName();
            try
            {
                using (var store1 = GetDocumentStore())
                using (var store2 = Sharding.GetDocumentStore())
                {
                    using (var session = store1.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User { Name = "Name1" }, "users/1");
                        await session.StoreAsync(new User { Name = "Name2" }, "users/2");

                        await session.SaveChangesAsync();
                    }

                    await store1.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition {Name = "t", Mode = PullReplicationMode.HubToSink, Disabled = true}));

                    var record = await store1.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store1.Database));
                    Assert.Equal(1, record.HubPullReplications.Count);
                 
                    var operation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    operation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    record = await store2.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store2.Database));
                    Assert.Equal(0, record.HubPullReplications.Count);
                }
            }
            finally
            {
                File.Delete(file);
            }
        }
    }
}

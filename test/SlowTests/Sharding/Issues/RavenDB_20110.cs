using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.Integrations.PostgreSQL;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Integrations.PostgreSQL;
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

        [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Sharding | RavenTestCategory.Replication)]
        public async Task ShouldSkipUnsupportedFeaturesInShardingOnImport_HubSinkReplication()
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

                    await store1.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition { Name = "t", Mode = PullReplicationMode.HubToSink, Disabled = true }));

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


        [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Sharding | RavenTestCategory.PostgreSql)]
        public async Task ShouldSkipUnsupportedFeaturesInShardingOnImport_PostgreSqlIntegration()
        {
            using (var srcStore = GetDocumentStore())
            using (var dstStore = Sharding.GetDocumentStore())
            {
                srcStore.Maintenance.Send(new ConfigurePostgreSqlOperation(new PostgreSqlConfiguration
                {
                    Authentication = new PostgreSqlAuthenticationConfiguration()
                    {
                        Users = new List<PostgreSqlUser>()
                        {
                            new PostgreSqlUser()
                            {
                                Username = "arek",
                                Password = "foo!@22"
                            }
                        }
                    }
                }));

                var record = await srcStore.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(srcStore.Database));

                Assert.NotNull(record.Integrations);
                Assert.NotNull(record.Integrations.PostgreSql);
                Assert.Equal(1, record.Integrations.PostgreSql.Authentication.Users.Count);

                Assert.Contains("arek", record.Integrations.PostgreSql.Authentication.Users.First().Username);
                Assert.Contains("foo!@22", record.Integrations.PostgreSql.Authentication.Users.First().Password);

                var exportFile = GetTempFileName();

                var exportOperation = await srcStore.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), exportFile);
                await exportOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                var operation = await dstStore.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), exportFile);
                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                record = await dstStore.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(dstStore.Database));

                Assert.Null(record.Integrations);
            }
        }
    }
}

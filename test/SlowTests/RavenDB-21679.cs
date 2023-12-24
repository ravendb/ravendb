using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.Expiration;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server.ServerWide.Context;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests
{
    public class RavenDB_21679 : RavenTestBase
    {
        public RavenDB_21679(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Certificates | RavenTestCategory.BackupExportImport)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanNotImportRestrictedFeaturesWithUserCertificate(Options options)
        {
            DoNotReuseServer();

            var file = Path.GetTempFileName();
            var dbName = GetDatabaseName();
            var dbName2 = dbName + "1";
            var certificates = Certificates.SetupServerAuthentication(serverUrl: null);
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
            var clientCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>
            {
                [dbName] = DatabaseAccess.Admin
            });
            var userCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate3.Value, new Dictionary<string, DatabaseAccess>
            {
                [dbName2] = DatabaseAccess.ReadWrite
            });
            IndexDefinition input;
            using (var store = GetDocumentStore(new Options()
                   {
                       AdminCertificate = adminCert,
                       ClientCertificate = clientCert,
                       ModifyDatabaseName = _ => dbName
                   }))
            {
                input = new IndexDefinition
                {
                    Maps = { "from user in docs.UserAndAges select new { user.Name }" },
                    Type = IndexType.Map,
                    Name = "Users_ByName"
                };

                await store
                    .Maintenance
                    .SendAsync(new PutIndexesOperation(new[] { input }));

                var ravenConnectionString = new RavenConnectionString()
                {
                    Name = "RavenConnectionString",
                    TopologyDiscoveryUrls = new[] { "http://localhost:8080" },
                    Database = "Northwind",
                };
                var result0 = store.Maintenance.Send(new PutConnectionStringOperation<RavenConnectionString>(ravenConnectionString));
                Assert.NotNull(result0.RaftCommandIndex);

                var backupPath = NewDataPath(suffix: "BackupFolder");
                var expirationConfiguration = new ExpirationConfiguration
                {
                    Disabled = false,
                    DeleteFrequencyInSec = 100,
                };

                await ExpirationHelper.SetupExpiration(store, Server.ServerStore, expirationConfiguration);

                var backupConfiguration = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "* */1 * * *", incrementalBackupFrequency: "* */2 * * *");
                await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(backupConfiguration));

                store.Maintenance.Send(new PutClientConfigurationOperation(new ClientConfiguration { ReadBalanceBehavior = ReadBalanceBehavior.None, LoadBalanceBehavior = LoadBalanceBehavior.None, LoadBalancerContextSeed = 0, Disabled = false }));

                await RevisionsHelper.SetupRevisionsAsync(store);

                DatabaseRecord record;
                using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                {
                    record = Server.ServerStore.Cluster.ReadDatabase(context, store.Database);
                }

                Assert.True(record.RavenConnectionStrings.ContainsKey("RavenConnectionString"));
                Assert.NotNull(record.Expiration);
                Assert.True(record.PeriodicBackups.Count > 0);
                Assert.NotNull(record.Client);
                Assert.NotNull(record.Revisions);

                var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
            }

            using (var store = GetDocumentStore(new Options()
            {
                AdminCertificate = adminCert,
                ClientCertificate = userCert,
                ModifyDatabaseName = _ => dbName2
            }))
            {
                WaitForUserToContinueTheTest(store, clientCert:userCert);
                var operation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                var output = await store
                    .Maintenance
                    .SendAsync(new GetIndexOperation(input.Name));

                Assert.Null(output);

                DatabaseRecord record;
                using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                {
                    record = Server.ServerStore.Cluster.ReadDatabase(context, store.Database);
                }

                Assert.False(record.RavenConnectionStrings.ContainsKey("RavenConnectionString"));
                Assert.Null(record.Expiration);
                Assert.False(record.PeriodicBackups.Count > 0);
                Assert.Null(record.Client);
                Assert.Null(record.Revisions);
            }
        }

        [RavenTheory(RavenTestCategory.Certificates)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task SampleDataWithDifferentCertificates(Options options)
        {
            DoNotReuseServer();

            var dbName = GetDatabaseName();
            var dbName2 = dbName + "1";
            var certificates = Certificates.SetupServerAuthentication(serverUrl: null);
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
            var clientCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>
            {
                [dbName] = DatabaseAccess.Admin
            });
            var userCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate3.Value, new Dictionary<string, DatabaseAccess>
            {
                [dbName2] = DatabaseAccess.ReadWrite
            });

            using (var store = GetDocumentStore(new Options()
            {
                AdminCertificate = adminCert,
                ClientCertificate = clientCert,
                ModifyDatabaseName = _ => dbName
            }))
            {
                await store.Maintenance.SendAsync(new CreateSampleDataOperation(operateOnTypes: DatabaseSmugglerOptions.DefaultOperateOnTypes));
                var indexDefinitions = store.Maintenance.Send(new GetIndexesOperation(0, 10));
                Assert.Equal(7, indexDefinitions.Length);
            }

            using (var store = GetDocumentStore(new Options()
            {
                AdminCertificate = adminCert,
                ClientCertificate = userCert,
                ModifyDatabaseName = _ => dbName2
            }))
            {
                await store.Maintenance.SendAsync(new CreateSampleDataOperation(operateOnTypes: DatabaseSmugglerOptions.DefaultOperateOnTypes));
                var indexDefinitions = store.Maintenance.Send(new GetIndexesOperation(0, 10));
                Assert.Equal(0, indexDefinitions.Length);
            }
        }
    }
}

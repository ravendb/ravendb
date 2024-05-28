using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
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
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server.Documents;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Smuggler.Documents.Data;
using Sparrow.Utils;
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
            IndexDefinition input = await InitInfoAndExport(adminCert, clientCert, dbName, file);

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

        private async Task<IndexDefinition> InitInfoAndExport(X509Certificate2 adminCert, X509Certificate2 clientCert, string dbName, string file)
        {
            IndexDefinition input;
            using (var store = GetDocumentStore(new Options() { AdminCertificate = adminCert, ClientCertificate = clientCert, ModifyDatabaseName = _ => dbName }))
            {
                input = new IndexDefinition { Maps = { "from user in docs.UserAndAges select new { user.Name }" }, Type = IndexType.Map, Name = "Users_ByName" };

                await store
                    .Maintenance
                    .SendAsync(new PutIndexesOperation(new[] { input }));

                var ravenConnectionString = new RavenConnectionString()
                {
                    Name = "RavenConnectionString", TopologyDiscoveryUrls = new[] { "http://localhost:8080" }, Database = "Northwind",
                };
                var result0 = store.Maintenance.Send(new PutConnectionStringOperation<RavenConnectionString>(ravenConnectionString));
                Assert.NotNull(result0.RaftCommandIndex);

                var backupPath = NewDataPath(suffix: "BackupFolder");
                var expirationConfiguration = new ExpirationConfiguration { Disabled = false, DeleteFrequencyInSec = 100, };

                await ExpirationHelper.SetupExpiration(store, Server.ServerStore, expirationConfiguration);

                var backupConfiguration = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "* */1 * * *", incrementalBackupFrequency: "* */2 * * *");
                await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(backupConfiguration));

                store.Maintenance.Send(new PutClientConfigurationOperation(new ClientConfiguration
                {
                    ReadBalanceBehavior = ReadBalanceBehavior.None, LoadBalanceBehavior = LoadBalanceBehavior.None, LoadBalancerContextSeed = 0, Disabled = false
                }));

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

            return input;
        }

        [RavenTheory(RavenTestCategory.Certificates | RavenTestCategory.BackupExportImport)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanNotImportRestrictedFeaturesWithUserCertificateLogs(Options options)
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
            await InitInfoAndExport(adminCert, clientCert, dbName, file);

            using (GetDocumentStore(new Options()
               {
                   AdminCertificate = adminCert,
                   ClientCertificate = userCert,
                   ModifyDatabaseName = _ => dbName2
               }))
            {
                using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
                await using (var inputStream = File.OpenRead(file))
                await using (var stream = ZstdStream.Decompress(inputStream))
                {
                    Assert.NotNull(stream);

                    using (DocumentDatabase database = Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(dbName2).Result)
                    using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    using (var source = new StreamSource(stream, context, database.Name, new DatabaseSmugglerOptionsServerSide(AuthorizationStatus.ValidUser)))
                    {
                        var destination = database.Smuggler.CreateDestination();
                        var smuggler = database.Smuggler.Create(source, destination, context, new DatabaseSmugglerOptionsServerSide(AuthorizationStatus.ValidUser));
                        var result = await smuggler.ExecuteAsync().WithCancellation(cts.Token);

                        Assert.Equal(5, result.DatabaseRecord.ErroredCount);
                        Assert.Equal(1, result.Indexes.ErroredCount);

                        var featuresList = new List<string>()
                        {
                            "periodic backup",
                            "Revision configuration",
                            "Expiration",
                            "Raven Connection Strings",
                            "Client Configuration"
                        };
                        foreach (var feature in featuresList)
                        {
                            Assert.True(result.Messages.Any(x => x.Contains($"Import of {feature} was skipped due to insufficient permissions on your current certificate.")));
                        }
                    }
                }
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

        [RavenTheory(RavenTestCategory.Certificates | RavenTestCategory.BackupExportImport)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanImportRestrictedFeaturesWithAdminCertificate(Options options)
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
                [dbName2] = DatabaseAccess.Admin
            });
            IndexDefinition input = await InitInfoAndExport(adminCert, clientCert, dbName, file);

            using (var store = GetDocumentStore(new Options()
            {
                AdminCertificate = adminCert,
                ClientCertificate = userCert,
                ModifyDatabaseName = _ => dbName2
            }))
            {
                var operation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                var output = await store
                    .Maintenance
                    .SendAsync(new GetIndexOperation(input.Name));

                Assert.NotNull(output);

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
            }
        }

        [RavenTheory(RavenTestCategory.Certificates | RavenTestCategory.BackupExportImport)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanImportRestrictedFeaturesWithOperator(Options options)
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
                [dbName2] = DatabaseAccess.Read
            }, SecurityClearance.Operator);
            IndexDefinition input = await InitInfoAndExport(adminCert, clientCert, dbName, file);

            using (var store = GetDocumentStore(new Options()
            {
                AdminCertificate = adminCert,
                ClientCertificate = userCert,
                ModifyDatabaseName = _ => dbName2
            }))
            {
                var operation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                var output = await store
                    .Maintenance
                    .SendAsync(new GetIndexOperation(input.Name));

                Assert.NotNull(output);

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
            }
        }
    }
}

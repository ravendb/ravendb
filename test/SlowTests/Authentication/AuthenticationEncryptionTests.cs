using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server.Documents.Indexes.Static;
using SlowTests.Voron.Compaction;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Authentication
{
    public class AuthenticationEncryptionTests : RavenTestBase
    {
        [Fact]
        public async Task CanUseEncryption()
        {
            var serverCertPath = SetupServerAuthentication();
            var dbName = GetDatabaseName();
            var adminCert = AskServerForClientCertificate(serverCertPath, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);

            var buffer = new byte[32];
            using (var rand = RandomNumberGenerator.Create())
            {
                rand.GetBytes(buffer);
            }
            var base64Key = Convert.ToBase64String(buffer);

            // sometimes when using `dotnet xunit` we get platform not supported from ProtectedData
            try
            {
                ProtectedData.Protect(Encoding.UTF8.GetBytes("Is supported?"), null, DataProtectionScope.CurrentUser);
            }
            catch (PlatformNotSupportedException)
            {
                // so we fall back to a file
                Server.ServerStore.Configuration.Security.MasterKeyPath = GetTempFileName();
            }

            Server.ServerStore.PutSecretKey(base64Key, dbName, true);

            using (var store = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = s => dbName,
                ModifyDatabaseRecord = record => record.Encrypted = true,
                Path = NewDataPath()
            }))
            {
                store.Maintenance.Send(new CreateSampleDataOperation());

                WaitForIndexing(store);

                var file = GetTempFileName();
                var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                using (var commands = store.Commands())
                {
                    var result = commands.Query(new IndexQuery
                    {
                        Query = "FROM @all_docs",
                        WaitForNonStaleResults = true
                    });
                    WaitForIndexing(store);


                    Assert.True(result.Results.Length > 1000);

                    QueryResult queryResult = store.Commands().Query(new IndexQuery
                    {
                        Query = "FROM INDEX 'Orders/ByCompany'"
                    });
                    QueryResult queryResult2 = store.Commands().Query(new IndexQuery
                    {
                        Query = "FROM INDEX 'Orders/Totals'"
                    });
                    QueryResult queryResult3 = store.Commands().Query(new IndexQuery
                    {
                        Query = "FROM INDEX 'Product/Search'"
                    });

                    Assert.True(queryResult.Results.Length > 0);
                    Assert.True(queryResult2.Results.Length > 0);
                    Assert.True(queryResult3.Results.Length > 0);
                }
            }
        }

        [Fact]
        public async Task CanRestartEncryptedDbWithIndexes()
        {
            var serverCertPath = SetupServerAuthentication();
            var dbName = GetDatabaseName();
            var adminCert = AskServerForClientCertificate(serverCertPath, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);

            var buffer = new byte[32];
            using (var rand = RandomNumberGenerator.Create())
            {
                rand.GetBytes(buffer);
            }
            var base64Key = Convert.ToBase64String(buffer);

            // sometimes when using `dotnet xunit` we get platform not supported from ProtectedData
            try
            {
                ProtectedData.Protect(Encoding.UTF8.GetBytes("Is supported?"), null, DataProtectionScope.CurrentUser);
            }
            catch (PlatformNotSupportedException)
            {
                // so we fall back to a file
                Server.ServerStore.Configuration.Security.MasterKeyPath = GetTempFileName();
            }

            Server.ServerStore.PutSecretKey(base64Key, dbName, true);

            using (var store = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = s => dbName,
                ModifyDatabaseRecord = record => record.Encrypted = true,
                Path = NewDataPath()
            }))
            {
                store.Maintenance.Send(new CreateSampleDataOperation());


                using (var commands = store.Commands())
                {
                    // create auto map index
                    var command = new QueryCommand(commands.Session, new IndexQuery
                    {
                        Query = "FROM Orders WHERE Lines.Count > 2",
                        WaitForNonStaleResults = true
                    });

                    commands.RequestExecutor.Execute(command, commands.Context);

                    // create auto map reduce index
                    command = new QueryCommand(commands.Session, new IndexQuery
                    {
                        Query = "FROM Orders GROUP BY Company WHERE count() > 5 SELECT count() as TotalCount",
                        WaitForNonStaleResults = true
                    });

                    commands.RequestExecutor.Execute(command, commands.Context);

                    var indexDefinitions = store.Maintenance.Send(new GetIndexesOperation(0, 10));

                    Assert.Equal(5, indexDefinitions.Length); // 3 sample data indexes + 2 new dynamic indexes

                    WaitForIndexing(store);

                    // perform a query per index
                    foreach (var indexDef in indexDefinitions)
                    {
                        QueryResult queryResult = store.Commands().Query(new IndexQuery
                        {
                            Query = $"FROM INDEX '{indexDef.Name}'"
                        });

                        Assert.True(queryResult.Results.Length > 0);
                    }

                    // restart database
                    Server.ServerStore.DatabasesLandlord.UnloadDirectly(store.Database);
                    await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);

                    // perform a query per index
                    foreach (var indexDef in indexDefinitions)
                    {
                        QueryResult queryResult = store.Commands().Query(new IndexQuery
                        {
                            Query = $"FROM INDEX '{indexDef.Name}'"
                        });

                        Assert.True(queryResult.Results.Length > 0);
                    }
                }
            }
        }

        [Fact]
        public async Task CanCompactEncryptedDb()
        {
            var serverCertPath = SetupServerAuthentication();
            var dbName = GetDatabaseName();
            var adminCert = AskServerForClientCertificate(serverCertPath, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);

            var buffer = new byte[32];
            using (var rand = RandomNumberGenerator.Create())
            {
                rand.GetBytes(buffer);
            }
            var base64Key = Convert.ToBase64String(buffer);

            // sometimes when using `dotnet xunit` we get platform not supported from ProtectedData
            try
            {
                ProtectedData.Protect(Encoding.UTF8.GetBytes("Is supported?"), null, DataProtectionScope.CurrentUser);
            }
            catch (PlatformNotSupportedException)
            {
                // so we fall back to a file
                Server.ServerStore.Configuration.Security.MasterKeyPath = GetTempFileName();
            }

            Server.ServerStore.PutSecretKey(base64Key, dbName, true);

            var path = NewDataPath();
            using (var store = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = s => dbName,
                ModifyDatabaseRecord = record => record.Encrypted = true,
                Path = path
            }))
            {
                store.Maintenance.Send(new CreateSampleDataOperation());

                for (int i = 0; i < 3; i++)
                {
                    await store.Operations.Send(new PatchByQueryOperation(new IndexQuery
                    {
                        Query = @"FROM Orders UPDATE { put(""orders/"", this); } "
                    })).WaitForCompletionAsync(TimeSpan.FromSeconds(300));
                }

                WaitForIndexing(store);

                var deleteOperation = store.Operations.Send(new DeleteByQueryOperation(new IndexQuery() { Query = "FROM orders" }));
                await deleteOperation.WaitForCompletionAsync(TimeSpan.FromSeconds(60));

                var oldSize = StorageCompactionTestsSlow.GetDirSize(new DirectoryInfo(path));

                var compactOperation = store.Maintenance.Server.Send(new CompactDatabaseOperation(new CompactSettings
                {
                    DatabaseName = store.Database,
                    Documents = true,
                    Indexes = new[] { "Orders/ByCompany", "Orders/Totals" }
                }));
                await compactOperation.WaitForCompletionAsync(TimeSpan.FromSeconds(60));

                var newSize = StorageCompactionTestsSlow.GetDirSize(new DirectoryInfo(path));

                Assert.True(oldSize > newSize);
            }
        }
    }
}

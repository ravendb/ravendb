using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server.Documents.Indexes.Static;
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
                Server.ServerStore.Configuration.Security.MasterKeyPath = Path.GetTempFileName();
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

                var file = Path.GetTempFileName();
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
                Server.ServerStore.Configuration.Security.MasterKeyPath = Path.GetTempFileName();
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
    }
}

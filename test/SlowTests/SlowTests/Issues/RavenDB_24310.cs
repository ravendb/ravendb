using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.ElasticSearch;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.ConnectionStrings;
using Raven.Server.Utils;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.SlowTests.Issues;

public class RavenDB_24310 : RavenTestBase
{
    public RavenDB_24310(ITestOutputHelper output) : base(output)
    {
        DoNotReuseServer();
    }

    [RavenFact(RavenTestCategory.Configuration)]
    public async Task CanCreateAndGetServerWideConnectionString()
    {
        using (var store = GetDocumentStore())
        {
            var ravenCS = new ServerWideConnectionString
            {
                ConnectionString = new RavenConnectionString
                {
                    Name = "MyRavenCS",
                    Database = "TargetDb",
                    TopologyDiscoveryUrls = new[] { "http://localhost:8080" }
                }
            };

            var result = await store.Maintenance.Server.SendAsync(new PutServerWideConnectionStringOperation(ravenCS));
            Assert.True(result.RaftCommandIndex > 0);

            var getResult = await store.Maintenance.Server.SendAsync(new GetServerWideConnectionStringsOperation("MyRavenCS", ConnectionStringType.Raven));
            Assert.Equal(1, getResult.Results.Count);
            Assert.Equal("MyRavenCS", getResult.Results[0].Name);
            Assert.Equal(ConnectionStringType.Raven, getResult.Results[0].Type);

            var innerCS = (RavenConnectionString)getResult.Results[0].ConnectionString;
            Assert.Equal("TargetDb", innerCS.Database);
            Assert.Equal("http://localhost:8080", innerCS.TopologyDiscoveryUrls[0]);
        }
    }

    [RavenFact(RavenTestCategory.Configuration)]
    public async Task ServerWideConnectionStringPropagatedToExistingDatabases()
    {
        using (var store = GetDocumentStore())
        {
            var db2Name = store.Database + "_second";
            await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(db2Name)));

            var ravenCS = new ServerWideConnectionString
            {
                ConnectionString = new RavenConnectionString
                {
                    Name = "MyRavenCS",
                    Database = "TargetDb",
                    TopologyDiscoveryUrls = new[] { "http://localhost:8080" }
                }
            };

            await store.Maintenance.Server.SendAsync(new PutServerWideConnectionStringOperation(ravenCS));

            var expectedName = ServerWideConnectionString.GetDatabaseRecordConnectionStringName("MyRavenCS");

            var record1 = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
            Assert.True(record1.RavenConnectionStrings.ContainsKey(expectedName));
            Assert.Equal("TargetDb", record1.RavenConnectionStrings[expectedName].Database);

            var record2 = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(db2Name));
            Assert.True(record2.RavenConnectionStrings.ContainsKey(expectedName));
            Assert.Equal("TargetDb", record2.RavenConnectionStrings[expectedName].Database);
        }
    }

    [RavenFact(RavenTestCategory.Configuration)]
    public async Task ServerWideConnectionStringPropagatedToNewDatabase()
    {
        using (var store = GetDocumentStore())
        {
            var ravenCS = new ServerWideConnectionString
            {
                ConnectionString = new RavenConnectionString
                {
                    Name = "MyRavenCS",
                    Database = "TargetDb",
                    TopologyDiscoveryUrls = new[] { "http://localhost:8080" }
                }
            };

            await store.Maintenance.Server.SendAsync(new PutServerWideConnectionStringOperation(ravenCS));

            // create a new database AFTER the server-wide connection string
            var newDbName = store.Database + "_new";
            await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(newDbName)));

            var expectedName = ServerWideConnectionString.GetDatabaseRecordConnectionStringName("MyRavenCS");

            var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(newDbName));
            Assert.True(record.RavenConnectionStrings.ContainsKey(expectedName));
            Assert.Equal("TargetDb", record.RavenConnectionStrings[expectedName].Database);
        }
    }

    [RavenFact(RavenTestCategory.Configuration)]
    public async Task CanUpdateServerWideConnectionString()
    {
        using (var store = GetDocumentStore())
        {
            var ravenCS = new ServerWideConnectionString
            {
                ConnectionString = new RavenConnectionString
                {
                    Name = "MyRavenCS",
                    Database = "TargetDb",
                    TopologyDiscoveryUrls = new[] { "http://localhost:8080" }
                }
            };

            await store.Maintenance.Server.SendAsync(new PutServerWideConnectionStringOperation(ravenCS));

            // update the connection string
            ravenCS.ConnectionString = new RavenConnectionString
            {
                Name = "MyRavenCS",
                Database = "UpdatedDb",
                TopologyDiscoveryUrls = new[] { "http://localhost:9090" }
            };

            await store.Maintenance.Server.SendAsync(new PutServerWideConnectionStringOperation(ravenCS));

            var expectedName = ServerWideConnectionString.GetDatabaseRecordConnectionStringName("MyRavenCS");

            var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
            Assert.True(record.RavenConnectionStrings.ContainsKey(expectedName));
            Assert.Equal("UpdatedDb", record.RavenConnectionStrings[expectedName].Database);
            Assert.Equal("http://localhost:9090", record.RavenConnectionStrings[expectedName].TopologyDiscoveryUrls[0]);
        }
    }

    [RavenFact(RavenTestCategory.Configuration)]
    public async Task CanDeleteServerWideConnectionString()
    {
        using (var store = GetDocumentStore())
        {
            var ravenCS = new ServerWideConnectionString
            {
                ConnectionString = new RavenConnectionString
                {
                    Name = "MyRavenCS",
                    Database = "TargetDb",
                    TopologyDiscoveryUrls = new[] { "http://localhost:8080" }
                }
            };

            await store.Maintenance.Server.SendAsync(new PutServerWideConnectionStringOperation(ravenCS));

            var expectedName = ServerWideConnectionString.GetDatabaseRecordConnectionStringName("MyRavenCS");

            // verify it was propagated
            var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
            Assert.True(record.RavenConnectionStrings.ContainsKey(expectedName));

            // delete it
            var deleteResult = await store.Maintenance.Server.SendAsync(new RemoveServerWideConnectionStringOperation<RavenConnectionString>(new RavenConnectionString { Name = "MyRavenCS" }));
            Assert.True(deleteResult.RaftCommandIndex > 0);

            // verify removed from server-wide
            var getResult = await store.Maintenance.Server.SendAsync(new GetServerWideConnectionStringsOperation("MyRavenCS", ConnectionStringType.Raven));
            Assert.Equal(0, getResult.Results.Count);

            // verify removed from database record
            record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
            Assert.False(record.RavenConnectionStrings.ContainsKey(expectedName));
        }
    }

    [RavenFact(RavenTestCategory.Configuration)]
    public async Task ExcludedDatabasesRespected()
    {
        using (var store = GetDocumentStore())
        {
            var db2Name = store.Database + "_excluded";
            await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(db2Name)));

            var ravenCS = new ServerWideConnectionString
            {
                ConnectionString = new RavenConnectionString
                {
                    Name = "MyRavenCS",
                    Database = "TargetDb",
                    TopologyDiscoveryUrls = new[] { "http://localhost:8080" }
                },
                ExcludedDatabases = new[] { db2Name }
            };

            await store.Maintenance.Server.SendAsync(new PutServerWideConnectionStringOperation(ravenCS));

            var expectedName = ServerWideConnectionString.GetDatabaseRecordConnectionStringName("MyRavenCS");

            // first database should have it
            var record1 = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
            Assert.True(record1.RavenConnectionStrings.ContainsKey(expectedName));

            // excluded database should NOT have it
            var record2 = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(db2Name));
            Assert.False(record2.RavenConnectionStrings.ContainsKey(expectedName));

            // clear the excluded list and verify the connection string is now propagated to the previously-excluded database
            ravenCS.ExcludedDatabases = null;
            await store.Maintenance.Server.SendAsync(new PutServerWideConnectionStringOperation(ravenCS));

            record2 = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(db2Name));
            Assert.True(record2.RavenConnectionStrings.ContainsKey(expectedName));
        }
    }

    [RavenFact(RavenTestCategory.Configuration)]
    public async Task CannotModifyServerWideConnectionStringViaDatabaseApi()
    {
        using (var store = GetDocumentStore())
        {
            var ravenCS = new ServerWideConnectionString
            {
                ConnectionString = new RavenConnectionString
                {
                    Name = "MyRavenCS",
                    Database = "TargetDb",
                    TopologyDiscoveryUrls = new[] { "http://localhost:8080" }
                }
            };

            await store.Maintenance.Server.SendAsync(new PutServerWideConnectionStringOperation(ravenCS));

            var prefixedName = ServerWideConnectionString.GetDatabaseRecordConnectionStringName("MyRavenCS");

            // attempt to PUT via database-level API should fail
            var dbLevelCS = new RavenConnectionString
            {
                Name = prefixedName,
                Database = "Hacked",
                TopologyDiscoveryUrls = new[] { "http://evil:8080" }
            };

            var ex = await Assert.ThrowsAsync<Raven.Client.Exceptions.RavenException>(async () =>
                await store.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(dbLevelCS)));
            Assert.StartsWith(ServerWideConnectionString.NamePrefix, prefixedName);

            // attempt to DELETE via database-level API should fail
            var removeCS = new RavenConnectionString
            {
                Name = prefixedName,
                Database = "TargetDb",
                TopologyDiscoveryUrls = new[] { "http://localhost:8080" }
            };

            ex = await Assert.ThrowsAsync<Raven.Client.Exceptions.RavenException>(async () =>
                await store.Maintenance.SendAsync(new RemoveConnectionStringOperation<RavenConnectionString>(removeCS)));
            Assert.StartsWith(ServerWideConnectionString.NamePrefix, prefixedName);
        }
    }

    [RavenFact(RavenTestCategory.Configuration)]
    public async Task MultipleTypesCoexist()
    {
        using (var store = GetDocumentStore())
        {
            var ravenCS = new ServerWideConnectionString
            {
                ConnectionString = new RavenConnectionString
                {
                    Name = "MyRavenCS",
                    Database = "TargetDb",
                    TopologyDiscoveryUrls = new[] { "http://localhost:8080" }
                }
            };

            var sqlCS = new ServerWideConnectionString
            {
                ConnectionString = new Raven.Client.Documents.Operations.ETL.SQL.SqlConnectionString
                {
                    Name = "MySqlCS",
                    ConnectionString = "Server=localhost;Database=test;",
                    FactoryName = "System.Data.SqlClient"
                }
            };

            await store.Maintenance.Server.SendAsync(new PutServerWideConnectionStringOperation(ravenCS));
            await store.Maintenance.Server.SendAsync(new PutServerWideConnectionStringOperation(sqlCS));

            // get all
            var allResult = await store.Maintenance.Server.SendAsync(new GetServerWideConnectionStringsOperation());
            Assert.Equal(2, allResult.Results.Count);
            Assert.Contains(allResult.Results, r => r.Name == "MyRavenCS" && r.Type == ConnectionStringType.Raven);
            Assert.Contains(allResult.Results, r => r.Name == "MySqlCS" && r.Type == ConnectionStringType.Sql);

            // verify both propagated to database
            var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));

            var ravenExpectedName = ServerWideConnectionString.GetDatabaseRecordConnectionStringName("MyRavenCS");
            var sqlExpectedName = ServerWideConnectionString.GetDatabaseRecordConnectionStringName("MySqlCS");

            Assert.True(record.RavenConnectionStrings.ContainsKey(ravenExpectedName));
            Assert.True(record.SqlConnectionStrings.ContainsKey(sqlExpectedName));

            // delete just the raven one (using the connection string returned by Get)
            var ravenServerWide = allResult.Results.First(r => r.Type == ConnectionStringType.Raven);
            await store.Maintenance.Server.SendAsync(new RemoveServerWideConnectionStringOperation<RavenConnectionString>((RavenConnectionString)ravenServerWide.ConnectionString));

            record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
            Assert.False(record.RavenConnectionStrings.ContainsKey(ravenExpectedName));
            Assert.True(record.SqlConnectionStrings.ContainsKey(sqlExpectedName));
        }
    }

    [RavenFact(RavenTestCategory.Configuration | RavenTestCategory.Etl)]
    public async Task CannotDeleteServerWideConnectionStringInUse()
    {
        using (var store = GetDocumentStore())
        {
            var ravenCS = new ServerWideConnectionString
            {
                ConnectionString = new RavenConnectionString
                {
                    Name = "MyRavenCS",
                    Database = "TargetDb",
                    TopologyDiscoveryUrls = new[] { "http://localhost:8080" }
                }
            };

            await store.Maintenance.Server.SendAsync(new PutServerWideConnectionStringOperation(ravenCS));

            var prefixedName = ServerWideConnectionString.GetDatabaseRecordConnectionStringName("MyRavenCS");

            // create an ETL task that uses the propagated connection string
            var etlConfig = new RavenEtlConfiguration
            {
                Name = "TestEtl",
                ConnectionStringName = prefixedName,
                Transforms =
                {
                    new Transformation
                    {
                        Name = "TestTransform",
                        Collections = { "Users" },
                        Script = "loadToUsers(this);"
                    }
                }
            };

            await store.Maintenance.SendAsync(new Raven.Client.Documents.Operations.ETL.AddEtlOperation<RavenConnectionString>(etlConfig));

            // attempt to delete server-wide connection string should fail
            var ex = await Assert.ThrowsAsync<Raven.Client.Exceptions.RavenException>(async () =>
                await store.Maintenance.Server.SendAsync(new RemoveServerWideConnectionStringOperation<RavenConnectionString>(new RavenConnectionString { Name = "MyRavenCS" })));
            Assert.Contains("It is used by", ex.Message, StringComparison.OrdinalIgnoreCase);
        }
    }

    [RavenFact(RavenTestCategory.Configuration | RavenTestCategory.BackupExportImport)]
    public async Task ServerWideConnectionStringFilteredOutDuringSnapshotRestore()
    {
        var backupPath = NewDataPath(suffix: "BackupFolder");

        using (var store = GetDocumentStore())
        {
            // create server-wide connection string
            var ravenCS = new ServerWideConnectionString
            {
                ConnectionString = new RavenConnectionString
                {
                    Name = "MyRavenCS",
                    Database = "TargetDb",
                    TopologyDiscoveryUrls = new[] { "http://localhost:8080" }
                }
            };

            await store.Maintenance.Server.SendAsync(new PutServerWideConnectionStringOperation(ravenCS));

            var expectedName = ServerWideConnectionString.GetDatabaseRecordConnectionStringName("MyRavenCS");

            // verify it was propagated
            var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
            Assert.True(record.RavenConnectionStrings.ContainsKey(expectedName));

            // create a snapshot backup
            var config = Backup.CreateBackupConfiguration(backupPath, backupType: BackupType.Snapshot);
            var backupTaskId = await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);

            // delete the server-wide connection string before restore so it won't be re-propagated
            await store.Maintenance.Server.SendAsync(
                new RemoveServerWideConnectionStringOperation<RavenConnectionString>(new RavenConnectionString { Name = "MyRavenCS" }));

            // restore to a new database
            var restoredDbName = $"restored_{store.Database}";
            var backupDirectory = Directory.GetDirectories(backupPath).First();

            using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration
            {
                BackupLocation = backupDirectory,
                DatabaseName = restoredDbName
            }))
            {
                // the restored database should NOT contain the server-wide connection string
                // because FilterOutServerWideTasks strips them during restore
                var restoredRecord = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(restoredDbName));
                Assert.False(restoredRecord.RavenConnectionStrings.ContainsKey(expectedName),
                    "Server-wide connection string should have been filtered out during snapshot restore");
            }
        }
    }

    [RavenFact(RavenTestCategory.Configuration | RavenTestCategory.Smuggler)]
    public async Task ServerWideConnectionStringFilteredOutDuringSmugglerExport()
    {
        var file = GetTempFileName();
        try
        {
            using (var store = GetDocumentStore())
            {
                // create server-wide connection string (propagated to the database)
                var ravenCS = new ServerWideConnectionString
                {
                    ConnectionString = new RavenConnectionString
                    {
                        Name = "MyRavenCS",
                        Database = "TargetDb",
                        TopologyDiscoveryUrls = new[] { "http://localhost:8080" }
                    }
                };

                await store.Maintenance.Server.SendAsync(new PutServerWideConnectionStringOperation(ravenCS));

                var expectedName = ServerWideConnectionString.GetDatabaseRecordConnectionStringName("MyRavenCS");

                // verify it was propagated
                var sourceRecord = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                Assert.True(sourceRecord.RavenConnectionStrings.ContainsKey(expectedName));

                // also add a regular (non-server-wide) connection string
                await store.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(
                    new RavenConnectionString
                    {
                        Name = "RegularCS",
                        Database = "SomeDb",
                        TopologyDiscoveryUrls = new[] { "http://localhost:9090" }
                    }));

                // export to file (the export should filter out server-wide connection strings)
                var exportOp = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                await exportOp.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                // delete the server-wide connection string so it won't be re-propagated
                await store.Maintenance.Server.SendAsync(
                    new RemoveServerWideConnectionStringOperation<RavenConnectionString>(new RavenConnectionString { Name = "MyRavenCS" }));

                // create a new database and import the file into it
                var targetDbName = store.Database + "_target";
                await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(targetDbName)));

                using (var targetStore = GetDocumentStore(new Options { CreateDatabase = false, ModifyDatabaseName = _ => targetDbName }))
                {
                    var importOp = await targetStore.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    await importOp.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    // the target database should NOT have the server-wide connection string
                    var targetRecord = await targetStore.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(targetDbName));
                    Assert.False(targetRecord.RavenConnectionStrings.ContainsKey(expectedName),
                        "Server-wide connection string should have been filtered out during smuggler export");

                    // but should have the regular connection string
                    Assert.True(targetRecord.RavenConnectionStrings.ContainsKey("RegularCS"),
                        "Regular connection string should have been imported");
                }
            }
        }
        finally
        {
            IOExtensions.DeleteFile(file);
        }
    }

    [RavenFact(RavenTestCategory.Configuration | RavenTestCategory.Etl)]
    public async Task ServerWideTestSqlConnection_BadConnectionString_ReturnsFailure()
    {
        using (var store = GetDocumentStore())
        {
            var result = await store.Maintenance.Server.SendAsync(
                new TestSqlConnectionStringOperation("System.Data.SqlClient", "Server=tcp:127.0.0.1:1;Connect Timeout=1"));

            Assert.False(result.Success);
            Assert.False(string.IsNullOrEmpty(result.Error));
        }
    }

    [RavenFact(RavenTestCategory.Configuration | RavenTestCategory.Etl)]
    public async Task ServerWideTestSnowflakeConnection_BadConnectionString_ReturnsFailure()
    {
        using (var store = GetDocumentStore())
        {
            var result = await store.Maintenance.Server.SendAsync(
                new TestSnowflakeConnectionStringOperation("account=invalid;user=invalid;password=invalid"));

            Assert.False(result.Success);
            Assert.False(string.IsNullOrEmpty(result.Error));
        }
    }

    [RavenFact(RavenTestCategory.Configuration | RavenTestCategory.Etl)]
    public async Task ServerWideTestElasticSearchConnection_UnreachableUrl_ReturnsFailure()
    {
        using (var store = GetDocumentStore())
        {
            var result = await store.Maintenance.Server.SendAsync(
                new TestElasticSearchConnectionStringOperation("http://127.0.0.1:1", authentication: null));

            Assert.False(result.Success);
            Assert.False(string.IsNullOrEmpty(result.Error));
        }
    }

    [RavenFact(RavenTestCategory.Configuration | RavenTestCategory.Etl)]
    public async Task ServerWideTestKafkaConnection_UnreachableBroker_ReturnsFailure()
    {
        using (var store = GetDocumentStore())
        {
            var result = await store.Maintenance.Server.SendAsync(
                new TestKafkaConnectionStringOperation(new KafkaConnectionSettings { BootstrapServers = "127.0.0.1:1" }));

            Assert.False(result.Success);
            Assert.False(string.IsNullOrEmpty(result.Error));
        }
    }

    [RavenFact(RavenTestCategory.Configuration | RavenTestCategory.Etl)]
    public async Task ServerWideTestRabbitMqConnection_BadConnectionString_ReturnsFailure()
    {
        using (var store = GetDocumentStore())
        {
            var result = await store.Maintenance.Server.SendAsync(
                new TestRabbitMqConnectionStringOperation(new RabbitMqConnectionSettings { ConnectionString = "amqp://guest:guest@127.0.0.1:1/" }));

            Assert.False(result.Success);
            Assert.False(string.IsNullOrEmpty(result.Error));
        }
    }

    [RavenFact(RavenTestCategory.Configuration | RavenTestCategory.Etl)]
    public async Task ServerWideTestAzureQueueStorageConnection_BadCredentials_ReturnsFailure()
    {
        using (var store = GetDocumentStore())
        {
            var result = await store.Maintenance.Server.SendAsync(
                new TestAzureQueueStorageConnectionStringOperation(new AzureQueueStorageConnectionSettings
                {
                    ConnectionString = "DefaultEndpointsProtocol=https;AccountName=invalid;AccountKey=aW52YWxpZA==;EndpointSuffix=core.windows.net"
                }));

            Assert.False(result.Success);
            Assert.False(string.IsNullOrEmpty(result.Error));
        }
    }

    [RavenFact(RavenTestCategory.Configuration | RavenTestCategory.Etl)]
    public async Task ServerWideTestAmazonSqsConnection_BadCredentials_ReturnsFailure()
    {
        using (var store = GetDocumentStore())
        {
            var result = await store.Maintenance.Server.SendAsync(
                new TestAmazonSqsConnectionStringOperation(new AmazonSqsConnectionSettings
                {
                    Basic = new AmazonSqsCredentials { AccessKey = "invalid", SecretKey = "invalid", RegionName = "us-east-1" }
                }));

            Assert.False(result.Success);
            Assert.False(string.IsNullOrEmpty(result.Error));
        }
    }

    [RavenFact(RavenTestCategory.Configuration | RavenTestCategory.Etl)]
    public async Task ServerWideTestAiConnection_UnreachableEndpoint_ReturnsFailure()
    {
        using (var store = GetDocumentStore())
        {
            var result = await store.Maintenance.Server.SendAsync(
                new TestAiConnectionStringOperation(AiConnectorType.Ollama, AiModelType.Chat, new OllamaSettings
                {
                    Uri = "http://127.0.0.1:1",
                    Model = "test"
                }));

            Assert.False(result.Success);
            Assert.False(string.IsNullOrEmpty(result.Error));
        }
    }
}

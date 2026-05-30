using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.Replication;
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
    public async Task DeletingNonExistentServerWideConnectionStringIsNoOp()
    {
        using (var store = GetDocumentStore())
        {
            // deleting a server-wide connection string that was never created should be a no-op and must not throw
            var deleteResult = await store.Maintenance.Server.SendAsync(
                new RemoveServerWideConnectionStringOperation<RavenConnectionString>(new RavenConnectionString { Name = "DoesNotExist" }));

            Assert.True(deleteResult.RaftCommandIndex > 0);

            // nothing should have been created or left behind
            var getResult = await store.Maintenance.Server.SendAsync(
                new GetServerWideConnectionStringsOperation("DoesNotExist", ConnectionStringType.Raven));
            Assert.Equal(0, getResult.Results.Count);
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
    public async Task AddingDatabaseToExcludedListRemovesPropagatedConnectionString()
    {
        using (var store = GetDocumentStore())
        {
            var db2Name = store.Database + "_excluded";
            await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(db2Name)));

            // create a server-wide connection string with no exclusions - propagated everywhere
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

            // both databases should have it
            var record1 = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
            Assert.True(record1.RavenConnectionStrings.ContainsKey(expectedName));

            var record2 = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(db2Name));
            Assert.True(record2.RavenConnectionStrings.ContainsKey(expectedName));

            // now add db2 to the excluded list and verify the connection string is removed from it
            ravenCS.ExcludedDatabases = new[] { db2Name };
            await store.Maintenance.Server.SendAsync(new PutServerWideConnectionStringOperation(ravenCS));

            record2 = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(db2Name));
            Assert.False(record2.RavenConnectionStrings.ContainsKey(expectedName),
                "Connection string should have been removed from the newly-excluded database");

            // the non-excluded database should still have it
            record1 = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
            Assert.True(record1.RavenConnectionStrings.ContainsKey(expectedName));
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
            Assert.Contains(prefixedName, ex.Message);
            Assert.Contains("create or update connection string", ex.Message, StringComparison.OrdinalIgnoreCase);

            // attempt to DELETE via database-level API should fail
            var removeCS = new RavenConnectionString
            {
                Name = prefixedName,
                Database = "TargetDb",
                TopologyDiscoveryUrls = new[] { "http://localhost:8080" }
            };

            ex = await Assert.ThrowsAsync<Raven.Client.Exceptions.RavenException>(async () =>
                await store.Maintenance.SendAsync(new RemoveConnectionStringOperation<RavenConnectionString>(removeCS)));
            Assert.Contains(prefixedName, ex.Message);
            Assert.Contains("can only be removed", ex.Message, StringComparison.OrdinalIgnoreCase);
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

    [RavenFact(RavenTestCategory.Configuration | RavenTestCategory.Replication)]
    public async Task CannotDeleteServerWideConnectionStringInUseBySinkPullReplication()
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

            // create a sink pull replication task that uses the propagated connection string
            await store.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(new PullReplicationAsSink
            {
                Name = "TestSink",
                ConnectionStringName = prefixedName,
                HubName = "TestHub"
            }));

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

    [RavenFact(RavenTestCategory.Configuration | RavenTestCategory.Etl)]
    public async Task GetServerWideConnectionStrings_ReturnsUsedByTasks()
    {
        using (var store = GetDocumentStore())
        {
            var csName = "MyServerWideRavenCS";
            var ravenCs = new ServerWideConnectionString
            {
                ConnectionString = new RavenConnectionString
                {
                    Name = csName,
                    Database = "TargetDb",
                    TopologyDiscoveryUrls = new[] { store.Urls[0] }
                }
            };
            await store.Maintenance.Server.SendAsync(new PutServerWideConnectionStringOperation(ravenCs));

            var prefixedCsName = $"Server Wide Connection String, {csName}";
            var etlConfig = new RavenEtlConfiguration
            {
                Name = "MyEtlTask",
                ConnectionStringName = prefixedCsName,
                Transforms = { new Transformation { Name = "t", Collections = { "Orders" }, Script = "" } }
            };
            var addEtlResult = await store.Maintenance.SendAsync(new AddEtlOperation<RavenConnectionString>(etlConfig));
            Assert.True(addEtlResult.TaskId > 0);

            var getResult = await store.Maintenance.Server.SendAsync(
                new GetServerWideConnectionStringsOperation(csName, ConnectionStringType.Raven));

            Assert.Equal(1, getResult.Results.Count);
            var cs = getResult.Results[0];
            Assert.Equal(csName, cs.Name);
            Assert.NotNull(cs.UsedBy);
            Assert.Equal(1, cs.UsedBy.Count);
            Assert.Equal(ConnectionStringUsageKind.RavenEtl, cs.UsedBy[0].Kind);
            Assert.Equal(addEtlResult.TaskId, cs.UsedBy[0].Id);
            Assert.Equal("MyEtlTask", cs.UsedBy[0].Name);
            Assert.Equal(store.Database, cs.UsedBy[0].DatabaseName);
        }
    }

    [RavenFact(RavenTestCategory.Configuration | RavenTestCategory.Etl)]
    public async Task GetConnectionStrings_ReturnsUsedByTasks()
    {
        using (var store = GetDocumentStore())
        {
            var cs = new RavenConnectionString
            {
                Name = "MyRavenCS",
                Database = "TargetDb",
                TopologyDiscoveryUrls = new[] { store.Urls[0] }
            };
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(cs));

            var etlConfig = new RavenEtlConfiguration
            {
                Name = "MyEtlTask",
                ConnectionStringName = "MyRavenCS",
                Transforms = { new Transformation { Name = "t", Collections = { "Orders" }, Script = "" } }
            };
            var addEtlResult = await store.Maintenance.SendAsync(new AddEtlOperation<RavenConnectionString>(etlConfig));
            Assert.True(addEtlResult.TaskId > 0);

            var getResult = await store.Maintenance.SendAsync(new GetConnectionStringsOperation());

            Assert.True(getResult.RavenConnectionStrings.ContainsKey("MyRavenCS"));
            var fetchedCs = getResult.RavenConnectionStrings["MyRavenCS"];

            Assert.NotNull(fetchedCs.UsedBy);
            Assert.Equal(1, fetchedCs.UsedBy.Count);
            Assert.Equal(ConnectionStringUsageKind.RavenEtl, fetchedCs.UsedBy[0].Kind);
            Assert.Equal(addEtlResult.TaskId, fetchedCs.UsedBy[0].Id);
            Assert.Equal("MyEtlTask", fetchedCs.UsedBy[0].Name);
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
    public async Task CannotExcludeDatabaseWhileServerWideConnectionStringInUse()
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

            // create an ETL task in this database that uses the propagated connection string
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

            // excluding this database would remove the in-use connection string - this must be blocked (just like a delete)
            ravenCS.ExcludedDatabases = new[] { store.Database };
            var ex = await Assert.ThrowsAsync<Raven.Client.Exceptions.RavenException>(async () =>
                await store.Maintenance.Server.SendAsync(new PutServerWideConnectionStringOperation(ravenCS)));
            Assert.Contains("It is used by", ex.Message, StringComparison.OrdinalIgnoreCase);

            // the failed operation must have rolled back: the connection string is still present in the database record
            var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
            Assert.True(record.RavenConnectionStrings.ContainsKey(prefixedName));
        }
    }

    [RavenFact(RavenTestCategory.Configuration | RavenTestCategory.Ai)]
    public async Task GetServerWideConnectionStrings_ReturnsUsedByTasks_GenAi()
    {
        using (var store = GetDocumentStore())
        {
            var csName = "MyServerWideAiCS";
            var aiConnectionString = new AiConnectionString
            {
                Name = csName,
                ModelType = AiModelType.Chat,
                OpenAiSettings = new OpenAiSettings { ApiKey = "fake-key", Model = "test" }
            };
            aiConnectionString.Identifier = aiConnectionString.GenerateIdentifier();

            await store.Maintenance.Server.SendAsync(new PutServerWideConnectionStringOperation(new ServerWideConnectionString { ConnectionString = aiConnectionString }));

            var prefixedCsName = ServerWideConnectionString.GetDatabaseRecordConnectionStringName(csName);

            // a GenAI task that uses the propagated AI connection string should be reported in UsedByTasks
            var genAiConfig = new GenAiConfiguration
            {
                Name = "MyGenAiTask",
                ConnectionStringName = prefixedCsName,
                Collection = "Users",
                Prompt = "Process users",
                SampleObject = "{\"Result\":\"test\"}",
                UpdateScript = "this.Result = $output.Result",
                GenAiTransformation = new GenAiTransformation { Script = "ai.genContext({ Name: this.Name });" }
            };
            var addResult = await store.Maintenance.SendAsync(new AddGenAiOperation(genAiConfig));
            Assert.True(addResult.TaskId > 0);

            var getResult = await store.Maintenance.Server.SendAsync(
                new GetServerWideConnectionStringsOperation(csName, ConnectionStringType.Ai));

            Assert.Equal(1, getResult.Results.Count);
            var cs = getResult.Results[0];
            Assert.Equal(csName, cs.Name);
            Assert.NotNull(cs.UsedBy);
            Assert.Equal(1, cs.UsedBy.Count);
            Assert.Equal(ConnectionStringUsageKind.GenAi, cs.UsedBy[0].Kind);
            Assert.Equal(addResult.TaskId, cs.UsedBy[0].Id);
            Assert.Equal("MyGenAiTask", cs.UsedBy[0].Name);
            Assert.Equal(store.Database, cs.UsedBy[0].DatabaseName);
        }
    }
}

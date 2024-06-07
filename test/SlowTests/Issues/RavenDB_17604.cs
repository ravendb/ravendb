using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Server.Utils;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_17604 : RavenTestBase
{
    public RavenDB_17604(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Etl)]
    public async Task Can_Disable_Etls_With_Marker()
    {
        var path = NewDataPath();
        IOExtensions.DeleteDirectory(path);

        using (var store = GetDocumentStore(new Options { RunInMemory = false, Path = path }))
        {
            var connectionStringName = Guid.NewGuid().ToString();
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
            {
                Name = connectionStringName,
                TopologyDiscoveryUrls = store.Urls,
                Database = connectionStringName
            }));

            await store.Maintenance.SendAsync(new AddEtlOperation<RavenConnectionString>(new RavenEtlConfiguration
            {
                ConnectionStringName = connectionStringName,
                Transforms = [new Transformation { Name = "MyScript", Collections = ["Orders"], Script = "loadToOrders(this);" }]
            }));

            var database = await GetDatabase(store.Database);
            Assert.Equal(1, database.EtlLoader.Processes.Length);
            Assert.True(database.EtlLoader.Processes[0].IsRunning);

            Server.ServerStore.DatabasesLandlord.UnloadDirectly(store.Database);

            database = await GetDatabase(store.Database);
            Assert.Equal(1, database.EtlLoader.Processes.Length);
            Assert.True(database.EtlLoader.Processes[0].IsRunning);

            Server.ServerStore.DatabasesLandlord.UnloadDirectly(store.Database);
            File.Create(Path.Combine(path, "disable.tasks.marker"));

            database = await GetDatabase(store.Database);
            Assert.Equal(1, database.EtlLoader.Processes.Length);
            Assert.False(database.EtlLoader.Processes[0].IsRunning);
        }
    }

    [RavenFact(RavenTestCategory.Etl)]
    public async Task Can_Disable_Backup_With_Marker()
    {
        var path = NewDataPath();
        IOExtensions.DeleteDirectory(path);

        using (var store = GetDocumentStore(new Options { RunInMemory = false, Path = path }))
        {
            var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(new PeriodicBackupConfiguration
            {
                LocalSettings = new LocalSettings { FolderPath = NewDataPath() },
                FullBackupFrequency = "* * * * *"
            }));

            await Backup.RunBackupAsync(Server, result.TaskId, store);

            Server.ServerStore.DatabasesLandlord.UnloadDirectly(store.Database);

            var database = await GetDatabase(store.Database);
            await WaitForValueAsync(() => database.PeriodicBackupRunner.PeriodicBackups.Count, 1);
            await Backup.RunBackupAsync(Server, result.TaskId, store);

            Server.ServerStore.DatabasesLandlord.UnloadDirectly(store.Database);
            File.Create(Path.Combine(path, "disable.tasks.marker"));
            database = await GetDatabase(store.Database);
            await WaitForValueAsync(() => database.PeriodicBackupRunner.PeriodicBackups.Count, 1);

            var e = await Assert.ThrowsAsync<InvalidOperationException>(() => Backup.RunBackupAsync(Server, result.TaskId, store));
            Assert.Contains("Backup task is disabled via marker file", e.Message);
        }
    }
}

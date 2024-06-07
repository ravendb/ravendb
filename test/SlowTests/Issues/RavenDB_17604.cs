using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Orders;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Server.Utils;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_17604 : ReplicationTestBase
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

    [RavenFact(RavenTestCategory.BackupExportImport)]
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
            Assert.Equal(1, await WaitForValueAsync(() => database.PeriodicBackupRunner.PeriodicBackups.Count, 1));
            await Backup.RunBackupAsync(Server, result.TaskId, store);

            Server.ServerStore.DatabasesLandlord.UnloadDirectly(store.Database);
            File.Create(Path.Combine(path, "disable.tasks.marker"));
            database = await GetDatabase(store.Database);
            Assert.Equal(1, await WaitForValueAsync(() => database.PeriodicBackupRunner.PeriodicBackups.Count, 1));

            var e = await Assert.ThrowsAsync<InvalidOperationException>(() => Backup.RunBackupAsync(Server, result.TaskId, store));
            Assert.Contains("Backup task is disabled via marker file", e.Message);
        }
    }

    [RavenFact(RavenTestCategory.Subscriptions)]
    public async Task Can_Disable_Subscription_With_Marker()
    {
        var path = NewDataPath();
        IOExtensions.DeleteDirectory(path);

        using (var store = GetDocumentStore(new Options { RunInMemory = false, Path = path }))
        {
            var name = await store.Subscriptions.CreateAsync<Order>();
            var worker = store.Subscriptions.GetSubscriptionWorker<Order>(name);

            var task = worker.Run(_ => { });

            var database = await GetDatabase(store.Database);
            Assert.Equal(1, await WaitForValueAsync(() => database.SubscriptionStorage.GetRunningCount(), 1));

            Server.ServerStore.DatabasesLandlord.UnloadDirectly(store.Database);

            database = await GetDatabase(store.Database);
            Assert.Equal(1, await WaitForValueAsync(() => database.SubscriptionStorage.GetRunningCount(), 1));

            Server.ServerStore.DatabasesLandlord.UnloadDirectly(store.Database);
            File.Create(Path.Combine(path, "disable.tasks.marker"));

            await GetDatabase(store.Database);
            var e = await Assert.ThrowsAsync<SubscriptionClosedException>(() => task.WaitAsync(TimeSpan.FromSeconds(30)));
            Assert.Contains("disabled", e.Message);
        }
    }

    [RavenFact(RavenTestCategory.Replication)]
    public async Task Can_Disable_ExternalReplication_With_Marker()
    {
        var path = NewDataPath();
        IOExtensions.DeleteDirectory(path);

        using (var store1 = GetDocumentStore(new Options { RunInMemory = false, Path = path }))
        using (var store2 = GetDocumentStore())
        {
            await SetupReplicationAsync(store1, store2);

            var database = await GetDatabase(store1.Database);

            using (var session = store1.OpenAsyncSession())
            {
                await session.StoreAsync(new Company(), "companies/1");
                await session.SaveChangesAsync();
            }

            Assert.NotNull(await WaitForDocumentToReplicateAsync<Company>(store2, "companies/1", TimeSpan.FromSeconds(15)));
            Assert.Equal(1, database.ReplicationLoader.OutgoingConnections.Count());

            Server.ServerStore.DatabasesLandlord.UnloadDirectly(store1.Database);

            database = await GetDatabase(store1.Database);

            using (var session = store1.OpenAsyncSession())
            {
                await session.StoreAsync(new Company(), "companies/2");
                await session.SaveChangesAsync();
            }

            Assert.NotNull(await WaitForDocumentToReplicateAsync<Company>(store2, "companies/2", TimeSpan.FromSeconds(15)));
            Assert.Equal(1, database.ReplicationLoader.OutgoingConnections.Count());

            Server.ServerStore.DatabasesLandlord.UnloadDirectly(store1.Database);
            File.Create(Path.Combine(path, "disable.tasks.marker"));

            await GetDatabase(store1.Database);

            using (var session = store1.OpenAsyncSession())
            {
                await session.StoreAsync(new Company(), "companies/3");
                await session.SaveChangesAsync();
            }

            Assert.Null(await WaitForDocumentToReplicateAsync<Company>(store2, "companies/3", TimeSpan.FromSeconds(3)));
            Assert.Equal(0, database.ReplicationLoader.OutgoingConnections.Count());
        }
    }
}

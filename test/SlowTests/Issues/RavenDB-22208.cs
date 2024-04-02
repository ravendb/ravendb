using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.Backups;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_22208 : RavenTestBase
    {
        public RavenDB_22208(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.TimeSeries)]
        public async Task Can_fix_collection_discrepancy_time_series_by_id()
        {
            using (var store = GetDocumentStore())
            {
                var backupPath = NewDataPath(forceCreateDir: true);
                var fullSnapshot = Path.Combine(backupPath, "northwind.ravendb-snapshot");

                using (var file = File.Create(fullSnapshot))
                using (var stream = typeof(RavenDB_10404).Assembly.GetManifestResourceStream("SlowTests.Data.RavenDB_22208.2024-04-01-16-40-54.ravendb-snapshot"))
                {
                    await stream.CopyToAsync(file);
                }

                var databaseName = GetDatabaseName();

                using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration
                {
                    BackupLocation = backupPath,
                    DatabaseName = databaseName
                }))
                {
                    const string docId = "users/1";
                    using (var session = store.OpenAsyncSession(databaseName))
                    {
                        var exists = await session.Advanced.ExistsAsync(docId);
                        Assert.True(exists);
                    }

                    await store.GetRequestExecutor().HttpClient.PostAsync($"{store.Urls.First()}/databases/{databaseName}/debug/documents/fix-collection-discrepancy?id={docId}", new StringContent("{'item': NaN}"));

                    using (var session = store.OpenAsyncSession(databaseName))
                    {
                        session.Delete("users/1");
                        await session.SaveChangesAsync();
                    }

                    using (var session = store.OpenAsyncSession(databaseName))
                    {
                        var exists = await session.Advanced.ExistsAsync(docId);
                        Assert.False(exists);
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.TimeSeries)]
        public async Task Can_fix_collection_discrepancy_time_series_by_collection()
        {
            using (var store = GetDocumentStore())
            {
                var backupPath = NewDataPath(forceCreateDir: true);
                var fullSnapshot = Path.Combine(backupPath, "northwind.ravendb-snapshot");

                using (var file = File.Create(fullSnapshot))
                using (var stream = typeof(RavenDB_10404).Assembly.GetManifestResourceStream("SlowTests.Data.RavenDB_22208.2024-04-01-16-40-54.ravendb-snapshot"))
                {
                    await stream.CopyToAsync(file);
                }

                var databaseName = GetDatabaseName();

                using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration
                       {
                           BackupLocation = backupPath,
                           DatabaseName = databaseName
                       }))
                {
                    const string docId = "users/1";
                    using (var session = store.OpenAsyncSession(databaseName))
                    {
                        var exists = await session.Advanced.ExistsAsync(docId);
                        Assert.True(exists);
                    }

                    await store.GetRequestExecutor().HttpClient.PostAsync($"{store.Urls.First()}/databases/{databaseName}/debug/documents/fix-collection-discrepancy?collection=Users", new StringContent("{'item': NaN}"));

                    using (var session = store.OpenAsyncSession(databaseName))
                    {
                        session.Delete("users/1");
                        await session.SaveChangesAsync();
                    }

                    using (var session = store.OpenAsyncSession(databaseName))
                    {
                        var exists = await session.Advanced.ExistsAsync(docId);
                        Assert.False(exists);
                    }
                }
            }
        }
    }
}

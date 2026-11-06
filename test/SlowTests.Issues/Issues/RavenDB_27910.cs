using System;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_27910 : RavenTestBase
    {
        public RavenDB_27910(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Voron | RavenTestCategory.BackupExportImport)]
        public async Task EstimatedCountOfDocumentsToProcessOnEtagIndexWrittenByOlderVersion()
        {
            // snapshot taken on 7.2.6 (508 entries per etag tree page) of a database where:
            // - users/1 .. users/258065 were bulk inserted, the last one split the root branch
            // - users/258065 was deleted, the right branch collapsed, so the root held a branch followed by a leaf
            // - users/258066 .. users/259335 were bulk inserted, 7.2.6 split that leaf sideways, so the root got more leaves after the branch
            var backupPath = NewDataPath(forceCreateDir: true);
            using (var file = File.Create(Path.Combine(backupPath, "estimation-7.2.6.ravendb-snapshot")))
            using (var resource = typeof(RavenDB_27910).Assembly.GetManifestResourceStream("SlowTests.Data.RavenDB_27910.estimation-7.2.6.ravendb-snapshot"))
                resource.CopyTo(file);

            using var store = GetDocumentStore();
            var database = GetDatabaseName();

            using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration { BackupLocation = backupPath, DatabaseName = database }))
            {
                // fills the last leaf and splits it, the current version wraps it in a branch, so leaves now precede the last branch of the root
                await using (var bulk = store.BulkInsert(database))
                {
                    for (var i = 259336; i <= 259590; i++)
                        await bulk.StoreAsync(new User(), $"users/{i}");
                }

                var db = await Databases.GetDocumentDatabaseInstanceFor(store, database);

                // past one second the count is estimated instead of counted
                var overallDuration = Stopwatch.StartNew();
                await Task.Delay(TimeSpan.FromSeconds(1.1));

                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    // starts in one of the leaves before the last branch
                    var afterEtag = db.DocumentsStorage.Get(context, "users/258319").Etag;
                    var count = db.DocumentsStorage.GetNumberOfDocumentsToProcess(context, "Users", afterEtag, out var totalCount, overallDuration);
                    Assert.InRange(count, 1, totalCount);
                }
            }
        }
    }
}

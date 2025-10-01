using System;
using System.Linq;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Backups;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_19625 : RavenTestBase
{
    public RavenDB_19625(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Indexes)]
    public async Task CanQueryIndexFilteredByDateTime()
    {
        using (var store = GetDocumentStore())
        {
            await store.ExecuteIndexAsync(new QueryDateTime_Index());

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Post { Id = "posts/1", Date = new DateTime(2023, 1, 1, 12, 11, 10) });

                await session.SaveChangesAsync();

                await Indexes.WaitForIndexingAsync(store);

                var res = await session.Query<QueryDateTime_Index.Result, QueryDateTime_Index>()
                    .Where(x => x.Date < DateTime.UtcNow)
                    .ProjectInto<QueryDateTime_Index.Result>()
                    .ToListAsync();

                Assert.NotEmpty(res);
                var hasTimeValues = (await GetDatabase(store.Database)).IndexStore.GetIndex(new QueryDateTime_Index().IndexName).IndexFieldsPersistence
                    .HasTimeValues(nameof(QueryDateTime_Index.Result.Date));
                Assert.True(hasTimeValues);
            }
        }
    }

    [RavenTheory(RavenTestCategory.Indexes)]
    [InlineData("SlowTests.Data.RavenDB_21957.js_index_with_dates_54112.ravendb-snapshot")]
    [InlineData("SlowTests.Data.RavenDB_21957.js_index_with_dates_601.ravendb-snapshot")]
    public async Task IndexBuiltBeforeJsDateIntroductionWillNotInsertTicks(string snapshotResourcePath)
    {
        var backupPath = NewDataPath(forceCreateDir: true);
        var fullBackupPath = Path.Combine(backupPath, "backup.ravendb-snapshot");

        using (var file = File.Create(fullBackupPath))
        using (var stream = typeof(RavenDB_19625).Assembly.GetManifestResourceStream(snapshotResourcePath))
        {
            await stream.CopyToAsync(file);
        }

        using var store = GetDocumentStore();
        var databaseName = GetDatabaseName();
        using var _ = Backup.RestoreDatabase(store, new RestoreBackupConfiguration {BackupLocation = backupPath, DatabaseName = databaseName});

        using (var session = store.OpenAsyncSession(databaseName))
        {
            var results = await session.Query<QueryDateTime_Index.Result, QueryDateTime_Index>()
                .Where(x => x.Date < new DateTime(2024, 1, 1))
                .ProjectInto<QueryDateTime_Index.Result>()
                .ToListAsync();
            Assert.Equal(1, results.Count);
            Assert.Equal("posts/1", results[0].Id);
            var hasTimeValues = (await GetDatabase(databaseName)).IndexStore.GetIndex(new QueryDateTime_Index().IndexName).IndexFieldsPersistence
                .HasTimeValues(nameof(QueryDateTime_Index.Result.Date));
            Assert.False(hasTimeValues);
            
            await session.StoreAsync(new Post {Id = "posts/2", Date = new DateTime(2023, 1, 2, 12, 11, 12)});
            await session.SaveChangesAsync();
            await Indexes.WaitForIndexingAsync(store, databaseName);
            
            results = await session.Query<QueryDateTime_Index.Result, QueryDateTime_Index>()
                .Where(x => x.Date < new DateTime(2024, 1, 1))
                .ProjectInto<QueryDateTime_Index.Result>()
                .ToListAsync();
            
            Assert.Equal(2, results.Count);

            hasTimeValues = (await GetDatabase(databaseName)).IndexStore.GetIndex(new QueryDateTime_Index().IndexName).IndexFieldsPersistence
                .HasTimeValues(nameof(QueryDateTime_Index.Result.Date));
            Assert.False(hasTimeValues);
        }
        
        
        WaitForUserToContinueTheTest(store, database: databaseName);
        
    }

    private class Post
    {
        public string Id { get; set; }
        public DateTime? Date { get; set; }
    }

    private class QueryDateTime_Index : AbstractJavaScriptIndexCreationTask
    {
        public class Result
        {
            public string Id { get; set; }
            public DateTime? Date { get; set; }
        }

        public QueryDateTime_Index()
        {
            Maps = new HashSet<string>
            {
                @"map('Posts', p => {
                    return {
                        Date: p.Date 
                    };
                });"
            };
        }
    }
}

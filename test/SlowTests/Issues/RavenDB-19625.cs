using System;
using System.Linq;
using System.Collections.Generic;
using System.IO;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Backups;
using Sparrow.Server.Utils;
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
    public void CanQueryIndexFilteredByDateTime()
    {
        using (var store = GetDocumentStore())
        {
            store.ExecuteIndex(new QueryDateTime_Index());

            using (var session = store.OpenSession())
            {
                session.Store(new Post {Id = "posts/1", Date = new DateTime(2023, 1, 1, 12, 11, 10)});

                session.SaveChanges();

                Indexes.WaitForIndexing(store);

                var res = session.Query<QueryDateTime_Index.Result, QueryDateTime_Index>()
                    .Where(x => x.Date < DateTime.UtcNow)
                    .ProjectInto<QueryDateTime_Index.Result>()
                    .ToList();

                Assert.NotEmpty(res);
                var hasTimeValues = GetDatabase(store.Database).Result.IndexStore.GetIndex(new QueryDateTime_Index().IndexName).IndexFieldsPersistence
                    .HasTimeValues(nameof(QueryDateTime_Index.Result.Date));
                Assert.True(hasTimeValues);
            }
        }
    }

    [RavenFact(RavenTestCategory.Indexes)]
    public void IndexBuiltBeforeJsDateIntroductionWillNotInsertTicks()
    {
        var backupPath = NewDataPath(forceCreateDir: true);
        var fullBackupPath = Path.Combine(backupPath, "54_001_index_ver.ravendb-snapshot");

        using (var file = File.Create(fullBackupPath))
        using (var stream = typeof(RavenDB_19625).Assembly.GetManifestResourceStream("SlowTests.Data.RavenDB_21957.js_index_with_dates_54112.ravendb-snapshot"))
        {
            stream.CopyTo(file);
        }

        using var store = GetDocumentStore();
        var databaseName = GetDatabaseName();
        using var _ = Backup.RestoreDatabase(store, new RestoreBackupConfiguration {BackupLocation = backupPath, DatabaseName = databaseName});

        using (var session = store.OpenSession(databaseName))
        {
            var results = session.Query<QueryDateTime_Index.Result, QueryDateTime_Index>()
                .Where(x => x.Date < new DateTime(2024, 1, 1))
                .ProjectInto<QueryDateTime_Index.Result>()
                .ToList();
            Assert.Equal(1, results.Count);
            Assert.Equal("posts/1", results[0].Id);
            var hasTimeValues = GetDatabase(databaseName).Result.IndexStore.GetIndex(new QueryDateTime_Index().IndexName).IndexFieldsPersistence
                .HasTimeValues(nameof(QueryDateTime_Index.Result.Date));
            Assert.False(hasTimeValues);
            
            
            session.Store(new Post {Id = "posts/2", Date = new DateTime(2023, 1, 2, 12, 11, 12)});
            session.SaveChanges();
            Indexes.WaitForIndexing(store, dbName: databaseName);
            
            results = session.Query<QueryDateTime_Index.Result, QueryDateTime_Index>()
                .Where(x => x.Date < new DateTime(2024, 1, 1))
                .ProjectInto<QueryDateTime_Index.Result>()
                .ToList();
            
            Assert.Equal(2, results.Count);

            hasTimeValues = GetDatabase(databaseName).Result.IndexStore.GetIndex(new QueryDateTime_Index().IndexName).IndexFieldsPersistence
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

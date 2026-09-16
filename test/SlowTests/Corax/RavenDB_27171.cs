using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Corax;

public class RavenDB_27171(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void DuplicatedNumericalValuesAreRemovedCorrectly(Options options)
    {
        using var store = GetDocumentStore(options);
        new ItemsIndex().Execute(store);

        var epoch = new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var ids = new List<string>();
        using (var session = store.OpenSession())
        {
            for (int i = 0; i < 100; i++)
            {
                var date = epoch.AddMinutes(i);
                var id = $"items/{i}";
                session.Store(new Item { Dates = new[] { date, date, date } }, id);
                ids.Add(id);
            }

            session.SaveChanges();
        }

        Indexes.WaitForIndexing(store);

        using (var session = store.OpenSession())
        {
            foreach (var id in ids)
                session.Delete(id);
            
            session.SaveChanges();
        }

        Indexes.WaitForIndexing(store);

        using (var session = store.OpenSession())
        {
            var remaining = session.Advanced.DocumentQuery<Item>(new ItemsIndex().IndexName)
                .OrderByDescending("Date", OrderingType.Long)
                .ToList();

            Assert.Empty(remaining);
        }
    }
    
    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void MixedTypeListValuesAreRemovedCorrectly(Options options)
    {
        using var store = GetDocumentStore(options);
        var index = new MixedItemsIndex();
        index.Execute(store);

        using (var session = store.OpenSession())
        {
            session.Store(new MixedItem { Values = new object[] { 5, "5" } }, "mixed/1");
            session.Store(new MixedItem { Values = new object[] { 5 } }, "mixed/2");
            session.Store(new MixedItem { Values = new object[] { 7, "7" } }, "mixed/3");
            session.SaveChanges();
        }

        Indexes.WaitForIndexing(store);

        using (var session = store.OpenSession())
        {
            session.Delete("mixed/1");
            session.Delete("mixed/2");
            session.Delete("mixed/3");
            session.SaveChanges();
        }

        Indexes.WaitForIndexing(store);

        var indexErrors = store.Maintenance.Send(new GetIndexErrorsOperation(new[] { index.IndexName }));
        Assert.Empty(indexErrors.SelectMany(x => x.Errors));

        using (var session = store.OpenSession())
        {
            var results = session.Advanced.DocumentQuery<MixedItem>(index.IndexName)
                .Statistics(out var stats)
                .WhereBetween("Value", 0L, 100L)
                .ToList();

            Assert.Empty(results);
            Assert.Equal(0, stats.TotalResults);
        }
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.BackupExportImport)]
    public void LegacyIndexRemovesDuplicatedNumericValuesAfterRestore()
    {
        var backupPath = NewDataPath(forceCreateDir: true);
        var file = Path.Combine(backupPath, "RavenDB_27171.ravendb-snapshot");

        using (var fileStream = File.Create(file))
        using (var stream = typeof(RavenDB_27171).Assembly.GetManifestResourceStream("SlowTests.Data.RavenDB_27171.RavenDB_27171.ravendb-snapshot"))
        {
            stream.CopyTo(fileStream);
        }

        using (var store = GetDocumentStore())
        {
            var databaseName = GetDatabaseName();
            using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration { BackupLocation = backupPath, DatabaseName = databaseName }))
            {
                using (var session = store.OpenSession(databaseName))
                {
                    for (int i = 0; i < 50; i++)
                        session.Delete($"items/{i}");

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store, databaseName);

                var indexErrors = store.Maintenance.ForDatabase(databaseName).Send(new GetIndexErrorsOperation(new[] { new ItemsIndex().IndexName }));
                Assert.Empty(indexErrors.SelectMany(x => x.Errors));

                using (var session = store.OpenSession(databaseName))
                {
                    var results = session.Advanced.DocumentQuery<Item>(new ItemsIndex().IndexName)
                        .Statistics(out var stats)
                        .OrderByDescending("Date", OrderingType.Long)
                        .ToList();

                    Assert.Empty(results);
                    Assert.Equal(0, stats.TotalResults);
                }
            }
        }
    }

    private class Item
    {
        public string Id { get; set; }
        public DateTime[] Dates { get; set; }
    }

    private class MixedItem
    {
        public string Id { get; set; }
        public object[] Values { get; set; }
    }

    private class ItemsIndex : AbstractIndexCreationTask<Item>
    {
        public ItemsIndex()
        {
            Map = items => from i in items
                select new { Date = i.Dates };
        }
    }

    private class MixedItemsIndex : AbstractIndexCreationTask<MixedItem>
    {
        public MixedItemsIndex()
        {
            Map = items => from i in items
                select new { Value = i.Values };
        }
    }
}

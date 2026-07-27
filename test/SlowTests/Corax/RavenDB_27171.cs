using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

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

    private class Item
    {
        public string Id { get; set; }
        public DateTime[] Dates { get; set; }
    }

    private class ItemsIndex : AbstractIndexCreationTask<Item>
    {
        public ItemsIndex()
        {
            Map = items => from i in items
                select new { Date = i.Dates };
        }
    }
}

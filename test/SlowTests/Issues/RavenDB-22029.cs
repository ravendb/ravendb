using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_22029 : RavenTestBase
{
    public RavenDB_22029(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void CanProjectViaJsStoredDate(Options options)
    {
        using var store = GetDocumentStore(options);
        var date = new DateTime(2024, 01, 01, 12, 15, 30);
        var dateList = Enumerable.Range(0, 10).Select(x => date.AddMinutes(x)).ToArray();
        using var session = store.OpenSession();
        session.Store(new Item(date, dateList));
        session.SaveChanges();
        
        var index = new Index();
        index.Execute(store);
        Indexes.WaitForIndexing(store);

        var results = session.Advanced.RawQuery<Item>($"from index '{index.IndexName}' as c select {{Date: c.Date}}").ToList();
        Assert.Equal(1, results.Count);
        Assert.Equal(date, results[0].Date);
        
        results = session.Advanced.RawQuery<Item>($"from index '{index.IndexName}' as c select {{DateList: c.DateList}}").ToList();
        Assert.Equal(1, results.Count);
        Assert.Equal(dateList, results[0].DateList);
    }
    
    
    private record Item(DateTime? Date, DateTime[] DateList, string Id = null);

    private class Index : AbstractIndexCreationTask<Item>
    {
        public Index()
        {
            Map = items => items.Select(x => new {x.Date, x.DateList});
            StoreAllFields(FieldStorage.Yes);
        }
    }
}

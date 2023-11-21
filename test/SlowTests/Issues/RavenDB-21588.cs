using System;
using System.Linq;
using FastTests;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_21588 : RavenTestBase
{
    public RavenDB_21588(ITestOutputHelper output) : base(output)
    {
    }

    private class DateAndTimeOnly
    {
        public TimeSpan TimeSpan { get; set; }
    }
    
    [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
    public void TimeSpanInBetweenQuery(Options options)
    {
        using var store = GetDocumentStore(options);

        var database = Enumerable.Range(0, 1000).Select(i => new DateAndTimeOnly() { TimeSpan = new TimeSpan(0, i, 0) }).ToList();

        using (var bulkInsert = store.BulkInsert())
        {
            database.ForEach(i => bulkInsert.Store(i));
        }
       
        using var session = store.OpenSession();
        var timeSpan2 = new TimeSpan(9, 0, 0);
        var timeSpan1 = new TimeSpan(8, 0, 0);

        var res1 = database.Where(p => p.TimeSpan >= timeSpan1 && p.TimeSpan <= timeSpan2).ToList();
        
        var res2 = session.Query<DateAndTimeOnly>().Customize(i => i.WaitForNonStaleResults()).Where(p => p.TimeSpan >= timeSpan1 && p.TimeSpan <= timeSpan2).ToList();
        
        Assert.Equal(res1.Count, res2.Count);

        Assert.All(res1, item => Assert.Contains(item.TimeSpan, res2.Select(x => x.TimeSpan)));
    }

    [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
    public void TimeSpanInGteQuery(Options options)
    {
        using var store = GetDocumentStore(options);
        
        var database = Enumerable.Range(0, 1000).Select(i => new DateAndTimeOnly() { TimeSpan = new TimeSpan(0, i, 0) }).ToList();

        using (var bulkInsert = store.BulkInsert())
        {
            database.ForEach(i => bulkInsert.Store(i));
        }

        using var session = store.OpenSession();
        var timeSpan1 = new TimeSpan(8, 0, 0);

        var res1 = database.Where(p => p.TimeSpan == timeSpan1).ToList();
        var res2 = session.Query<DateAndTimeOnly>().Customize(i => i.WaitForNonStaleResults()).Where(p => p.TimeSpan == timeSpan1).ToList();

        Assert.Equal(res1.Count, res2.Count);
        Assert.All(res1, item => Assert.Contains(item.TimeSpan, res2.Select(x => x.TimeSpan)));
    }
}

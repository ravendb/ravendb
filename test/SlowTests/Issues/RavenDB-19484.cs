using System;
using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_19484 : RavenTestBase
{
    public RavenDB_19484(ITestOutputHelper output) : base(output)
    {
    }
    
    [Fact]
    public void TestIncSeriesTwicePerSession()
    {
        using var store = GetDocumentStore();
        using var session = store.OpenSession();
        const string docId = "just-there-for-ref";
        session.Store(new { Id = docId});
        session.SaveChanges();
        

        var dateTime = new DateTimeOffset(2023, 08, 05, 15, 46, 10, TimeSpan.Zero).DateTime;
        session.IncrementalTimeSeriesFor(docId, "INC:MySeries")
            .Increment(dateTime,new double[]{ 0, 1});
        
        // does not increase somehow gets ignored
        session.IncrementalTimeSeriesFor(docId, "INC:MySeries")
            .Increment(dateTime,new double[]{ 0, 3});
        
        session.SaveChanges();
        var entries = session.IncrementalTimeSeriesFor(docId, "INC:MySeries")
            .Get();

        Assert.Equal(1, entries.Length);
        Assert.Equal(4, entries[0].Values[1]);
    }
}

using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_22960(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenTheory(RavenTestCategory.Facets)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void NullAndEmptyWillBeReturnedExactlyTheSameForAllEngines(Options options)
    {
        using var store = GetDocumentStore(options);
        using var session = store.OpenSession();
        session.Store(new Dto(Name: "test", NumericalValue: 1));
        session.Store(new Dto(Name: string.Empty, NumericalValue: 1));
        session.Store(new Dto(Name: null, NumericalValue: 1));
        
        session.SaveChanges();
        new Index().Execute(store);
        Indexes.WaitForIndexing(store);
        
        //Testing Aggregate via Index (Corax)
        var viaIndex = session.Query<Dto, Index>().AggregateBy(x => x.ByField(x => x.Name)).Execute();
        Assert.Equal(3, viaIndex["Name"].Values.Count);
        var ranges = viaIndex["Name"].Values.Select(x => x.Range).ToArray();
        Assert.Contains("test", ranges);
        Assert.Contains("EMPTY_STRING", ranges);
        Assert.Contains("NULL_VALUE", ranges);
        
        //aggregation On is not supported by index aggregation
        var viaScan = session.Query<Dto, Index>().AggregateBy(x => x.ByField(x => x.Name).SumOn(x => x.NumericalValue)).Execute();
        Assert.Equal(3, viaScan["Name"].Values.Count);
        ranges = viaScan["Name"].Values.Select(x => x.Range).ToArray();
        Assert.Equal(1, viaScan["Name"].Values.Select(x => x.Sum).Distinct().First());
        Assert.Contains("test", ranges);
        Assert.Contains("EMPTY_STRING", ranges);
        Assert.Contains("NULL_VALUE", ranges);
    }

    private record Dto(string Name, long NumericalValue, string Id = null);

    private class Index : AbstractIndexCreationTask<Dto>
    {
        public Index()
        {
            Map = docs => docs.Select(doc => new {doc.Name, doc.NumericalValue, Id = doc.Id});
        }
    }
}

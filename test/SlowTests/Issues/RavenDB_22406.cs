using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_22406 : RavenTestBase
{
    public RavenDB_22406(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
    public void CanIndexProperlyWithOneInvalidDocument(Options options)
    {
        using var store = GetDocumentStore(options);
        using var session = store.OpenSession();
        session.Store(new FanoutDto(){Inners = new []{new Inner(false, 1), new Inner(false, 2)}});
        session.Store(new FanoutDto(){Inners = new []{new Inner(false, 3), new Inner(false, 4), new Inner(true, 5)}});
        session.Store(new FanoutDto(){Inners = new []{new Inner(false, 6), new Inner(false, 7)}});
        new Index().Execute(store);
        session.Advanced.WaitForIndexesAfterSaveChanges();
        session.SaveChanges();
        
        var count = session.Query<FanoutDto, Index>().Count();
        Assert.Equal(4, count); //doc1 and doc3 only
        var terms = store
            .Maintenance
            .Send(new GetTermsOperation(new Index().IndexName, "id()", null, int.MaxValue));
        
        Assert.Equal(2, terms.Length);
    }

    private class FanoutDto
    {
        public Inner[] Inners { get; set; }
    }

    private record Inner(bool Throw, decimal Value);
    
    private class Index : AbstractIndexCreationTask<FanoutDto>
    {
        public Index()
        {
            Map = dtos => from dto in dtos
                from inner in dto.Inners
                select new
                {
                    Alphabet = inner.Value.ToString(),
                    Value = inner.Throw ? inner.Value / 0 : inner.Value
                };
        }
    }
}

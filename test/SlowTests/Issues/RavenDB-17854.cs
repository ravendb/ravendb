using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Facets;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_17854 : RavenTestBase
{
    private const string Exception = "Method WithOptions is not supported for aggregation 'ByRanges'";
    
    public RavenDB_17854(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Facets)]
    public void CanMixByRangeAndByField()
    {
        IAggregationQuery<DataForAggregation> Query(IDocumentSession session) =>
            session.Query<DataForAggregation, Index>()
                .AggregateBy(builder => builder.ByField(i => i.Name))
                .AndAggregateBy(builder => builder.ByRanges(i => i.Age < 23, aggregation => aggregation.Age >= 24));

        RunTest(Query);
    }

    [RavenFact(RavenTestCategory.Facets)]
    public void UseOptionsForByFieldButNotForByRange()
    {
        IAggregationQuery<DataForAggregation> Query(IDocumentSession session) =>
            session.Query<DataForAggregation, Index>()
                .AggregateBy(builder => builder.ByField(i => i.Name).WithOptions(new FacetOptions(){IncludeRemainingTerms = true}))
                .AndAggregateBy(builder => builder.ByRanges(i => i.Age < 23, aggregation => aggregation.Age >= 24));
        
        RunTest(Query);
    }
    
    [RavenFact(RavenTestCategory.Facets)]
    public void CanUseOptionWithByField()
    {
        IAggregationQuery<DataForAggregation> Query(IDocumentSession session) =>
            session.Query<DataForAggregation, Index>()
                .AggregateBy(builder => builder.ByField(i => i.Name).WithOptions(new FacetOptions(){IncludeRemainingTerms = true}));
        
        RunTest(Query, assertByRange: false);
    }
    
    
    [RavenFact(RavenTestCategory.Facets)]
    public void UseOptionWithByRangeWillCauseAException()
    {
        IAggregationQuery<DataForAggregation> Query(IDocumentSession session) =>
            session.Query<DataForAggregation, Index>()
                .AggregateBy(builder => builder.ByRanges(i => i.Age < 23, aggregation => aggregation.Age >= 24).WithOptions(new FacetOptions(){IncludeRemainingTerms = true}));


        var exception = Assert.ThrowsAny<NotSupportedException>(() => RunTest(Query, assertByField: false));
        Assert.True(exception.Message.Contains(Exception));
    }

    [RavenFact(RavenTestCategory.Facets)]
    public void UseOptionInByRangeAndByFieldWillCauseAException()
    {
        IAggregationQuery<DataForAggregation> Query(IDocumentSession session) =>
            session.Query<DataForAggregation, Index>()
                .AggregateBy(builder => builder.ByField(i => i.Name).WithOptions(new FacetOptions(){IncludeRemainingTerms = true}))
                .AndAggregateBy(builder => builder.ByRanges(i => i.Age < 23, aggregation => aggregation.Age >= 24).WithOptions(new FacetOptions(){IncludeRemainingTerms = true}));


        var exception = Assert.ThrowsAny<NotSupportedException>(() => RunTest(Query));
        Assert.True(exception.Message.Contains(Exception));
    }
    
    private void RunTest(Func<IDocumentSession, IAggregationQuery<DataForAggregation>> query, bool assertByField = true, bool assertByRange = true)
    {
        using var store = GetStoreWithPreparedData();
        {
            using var session = store.OpenSession();
            var results = query(session).Execute();
            if (assertByField)
                Assert.Equal(2, results["Name"].Values.Count);
            
            if (assertByRange)
                Assert.Equal(2, results["Age"].Values.Count);
        }
    }
    
    private IDocumentStore GetStoreWithPreparedData()
    {
        var store = GetDocumentStore();
        {
            using var session = store.OpenSession();
            session.Store(new DataForAggregation() {Age = 21, Name = "Matt"});
            session.Store(new DataForAggregation() {Age = 24, Name = "Tom"});
            session.SaveChanges();
        }

        new Index().Execute(store);
        Indexes.WaitForIndexing(store);
        return store;
    }

    private class DataForAggregation
    {
        public string Name { get; set; }
        public int Age { get; set; }
    }

    private class Index : AbstractIndexCreationTask<DataForAggregation>
    {
        public Index()
        {
            Map = enumerable => enumerable.Select(i => new DataForAggregation() {Name = i.Name, Age = i.Age});
        }
    }
}

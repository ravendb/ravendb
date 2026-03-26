using System.Linq;
using FastTests;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Corax;

public class CoraxNotExistingTests : RavenTestBase
{
    public CoraxNotExistingTests(ITestOutputHelper output) : base(output)
    {
    }
    
    private class TestObj
    {
        public string Id { get; set; }
        public string Prop { get; set; }
        public string Prop1 { get; set; }
    }
    
    private class WithExtraPropTestObj : TestObj
    {
        public string ExtraProp { get; set; }
    }
    
    private class TestIndex : AbstractIndexCreationTask<WithExtraPropTestObj, TestIndex.Result>
    {
        public class Result
        {
            public string Prop { get; set; }
            public string Prop1 { get; set; }
            public string ExtraProp { get; set; }
        }

        public TestIndex()
        {
            Map = users => from o in users
                select new Result
                {
                    Prop = o.Prop,
                    Prop1 = o.Prop1,
                    ExtraProp = o.ExtraProp
                };
        }
    }
    
    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void Corax_WhenQueryNotExisting_ShouldNotThrow(Options options)
    {
        const string somevalue = "somevalue";

        options.ModifyDocumentStore = (s) =>
        {
            s.Conventions = new DocumentConventions { FindCollectionName = _ => "TestObjs" };
        };
        using var store = GetDocumentStore(options);

        var testIndex = new TestIndex();
        testIndex.Execute(store);
        
        using (var session = store.OpenSession())
        {
            session.Store(new TestObj
            {
                Prop = somevalue,
                Prop1 = somevalue,
                //ExtraProp not existing
            });
            session.Store(new WithExtraPropTestObj
            {
                Prop = somevalue,
                Prop1 = somevalue,
                ExtraProp = null
            });
            session.Store(new WithExtraPropTestObj
            {
                Prop = somevalue,
                Prop1 = somevalue,
                ExtraProp = somevalue
            });
            session.SaveChanges();
        }
        
        Indexes.WaitForIndexing(store);
        
        using (var session = store.OpenSession())
        {
            string indexName =
                $"""
                 from index '{testIndex.IndexName}'
                 where exists(ExtraProp) and ExtraProp != null and Prop == "somevalue"  and  Prop1 == "somevalue"
                 order by ExtraProp
                 """;
            var results = session.Advanced.RawQuery<TestObj>(indexName).ToArray();
            Assert.Equal(1, results.Length);
        }
    }
}

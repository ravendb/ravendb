using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq.Indexing;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax;

public class RavenIntegration : RavenTestBase
{
    public RavenIntegration(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void CanIndexWithDocumentBoostAndDeleteTheItems(Options options)
    {
        using var store = GetDocumentStore(options);
        {
            using var session = store.OpenSession();
            session.Store(new Doc() {Name = "Two", BoostFactor = 1});
            session.Store(new Doc() {Name = "Three", BoostFactor = 3});
            session.Store(new Doc() {Name = "Four", BoostFactor = 4});
            session.SaveChanges();
        }

        new DocIndex().Execute(store);
        Indexes.WaitForIndexing(store);
        {
            using var session = store.OpenSession();
            var results = session.Query<Doc, DocIndex>().OrderByScore().ToList();
            Assert.Equal(results.Count, 3);
            Assert.Equal(results[0].Name, "Four");
            Assert.Equal(results[1].Name, "Three");
            Assert.Equal(results[2].Name, "Two");
        }

        
        {
            using var session = store.OpenSession();
            var doc = session.Query<Doc, DocIndex>().Single(i => i.Name == "Two");
            session.Delete(doc);
            session.SaveChanges();
        }
        
        Indexes.WaitForIndexing(store);
        {
            using var session = store.OpenSession();
            var results = session.Query<Doc, DocIndex>().OrderByScore().ToList();
            Assert.Equal(results.Count, 2);
            Assert.Equal(results[0].Name, "Four");
            Assert.Equal(results[1].Name, "Three");
        }
    }
    
    [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void CanUsePosititiveAndNegativeBoostFactors(Options options)
    {
        using var store = GetDocumentStore(options);
        {
            using var session = store.OpenSession();
            session.Store(new Doc() {Name = "Two", BoostFactor = -4});
            session.Store(new Doc() {Name = "Three", BoostFactor = -3});
            session.Store(new Doc() {Name = "Four", BoostFactor = -2});
            session.Store(new Doc() {Name = "Five", BoostFactor = 5});

            session.SaveChanges();
        }

        new DocIndex().Execute(store);
        Indexes.WaitForIndexing(store);
        {
            using var session = store.OpenSession();
            var results = session.Query<Doc, DocIndex>().OrderByScore().ToList();
            Assert.Equal(results.Count, 4);
            Assert.Equal(results[0].Name, "Five");
        }
    }
    
    [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void CanIndexWithDocumentBoostAndUpdateTheItems(Options options)
    {
        using var store = GetDocumentStore(options);
        {
            using var session = store.OpenSession();
            session.Store(new Doc() {Name = "Two", BoostFactor = 2});
            session.Store(new Doc() {Name = "Three", BoostFactor = 3});
            session.Store(new Doc() {Name = "Four", BoostFactor = 4});
            session.SaveChanges();
        }

        new DocIndex().Execute(store);
        Indexes.WaitForIndexing(store);
        {
            using var session = store.OpenSession();
            var results = session.Query<Doc, DocIndex>().OrderByScore().ToList();
            Assert.Equal(results.Count, 3);
            Assert.Equal(results[0].Name, "Four");
            Assert.Equal(results[1].Name, "Three");
            Assert.Equal(results[2].Name, "Two");
        }

        
        {
            using var session = store.OpenSession();
            var doc = session.Query<Doc, DocIndex>().Single(i => i.Name == "Two");
            doc.BoostFactor = 5;
            session.SaveChanges();
        }
        
        Indexes.WaitForIndexing(store);
        {
            using var session = store.OpenSession();
            var results = session.Query<Doc, DocIndex>().OrderByScore().ToList();
            Assert.Equal(results.Count, 3);
            Assert.Equal(results[0].Name, "Two");
            Assert.Equal(results[1].Name, "Four");
            Assert.Equal(results[2].Name, "Three");
        }
    }

    [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void IndexTimeDocumentBoostViaLinq(Options options) => IndexTimeDocumentBoost<DocIndex>(options);
    
    [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void IndexTimeDocumentBoostViaJs(Options options) => IndexTimeDocumentBoost<JsDocIndex>(options);
    
    private void IndexTimeDocumentBoost<T>(Options options, IDocumentStore defaultStore = null) where T : AbstractIndexCreationTask, new()
    {
        using var store = GetDocumentStore(options);
        {
            using var session = store.OpenSession();
            session.Store(new Doc() {Name = "Two", BoostFactor = 2});
            session.Store(new Doc() {Name = "Three", BoostFactor = 3});
            session.Store(new Doc() {Name = "Four", BoostFactor = 4});
            session.SaveChanges();
        }

        new T().Execute(store);
        Indexes.WaitForIndexing(store);
        {
            using var session = store.OpenSession();
            var results = session.Query<Doc, T>().OrderByScore().ToList();
            Assert.Equal(results.Count, 3);
            Assert.Equal(results[0].Name, "Four");
            Assert.Equal(results[1].Name, "Three");
            Assert.Equal(results[2].Name, "Two");
        }
    }

    private class DocIndex : AbstractIndexCreationTask<Doc>
    {
        public DocIndex()
        {
            Map = docs => from doc in docs
                select new {Name = doc.Name}.Boost(doc.BoostFactor);
        }
    }

    private class JsDocIndex : AbstractJavaScriptIndexCreationTask
    {
        public JsDocIndex()
        {
            Maps = new HashSet<string> {@"map('Docs', function (u){ return boost({ Name: u.Name}, u.BoostFactor);})",};
        }
    }

    private class Doc
    {
        public string Name { get; set; }
        public float BoostFactor { get; set; }
    }


    [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void CanMixValuesInQueryForBetween(Options options)
    {
        using var store = GetDocumentStore(options);
        {
            using var s = store.OpenSession();
            s.Store(new DoubleItem(2));
            s.SaveChanges();
        }

        {
            using var s = store.OpenSession();
            var q = s.Advanced.RawQuery<DoubleItem>("from DoubleItems where 'Value' < 3 and 'Value' > 1.5").ToList();
            Assert.Equal(1, q.Count);
        }
    }

    private record DoubleItem(double Value);
}

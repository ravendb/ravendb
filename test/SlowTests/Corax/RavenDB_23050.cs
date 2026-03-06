using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Corax;

public class RavenDB_23050(ITestOutputHelper output) : RavenTestBase(output)                                                                     
{
    [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void MustAnalyzeFieldsOnUpdateInIndexJsDynamicFields(Options options)
        => MustAnalyzeFieldsOnUpdateInIndex<IndexJsDynamic>(options);

    [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void MustAnalyzeFieldsOnUpdateInIndexJsStaticFields(Options options)
        => MustAnalyzeFieldsOnUpdateInIndex<IndexJsStaticFields>(options);

    [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void MustAnalyzeFieldsOnUpdateInIndexCsharpStaticFields(Options options)
        => MustAnalyzeFieldsOnUpdateInIndex<IndexCsharpDynamic>(options);

    private void MustAnalyzeFieldsOnUpdateInIndex<T>(Options options) where T : AbstractIndexCreationTask, new()
    {
        using var store = GetDocumentStore(options);
        using (var session = store.OpenSession())
        {
            session.Store(new Listing { Seller = "Abc" }, "listings/1");

            session.SaveChanges();
        }

        new T().Execute(store);
        Indexes.WaitForIndexing(store);
        
        using (var session = store.OpenSession())
        {
            var results = session.Query<Listing, T>()
                .Where(x => x.Seller == "Abc")
                .ToList();
            Assert.Equal(1, results.Count);
            
            var listing = session.Load<Listing>("listings/1");
            listing.Seller = "Def";
            session.SaveChanges();

            results = session.Query<Listing, T>()
                .Customize(x => x.WaitForNonStaleResults())
                .Where(x => x.Seller == "Def")
                .ToList();
            Assert.Equal(1, results.Count);
        }
    }

    private class Listing
    {
        public string Seller { get; set; }
    }

    private class IndexJsDynamic : AbstractJavaScriptIndexCreationTask
    {
        public IndexJsDynamic()
        {
            Maps = new HashSet<string>
            {
                @"map('Listings', listing => {

    const result = Object.assign({
        Seller: listing.Seller
    });

    return result
})",
            };
        }
    }

    private class IndexCsharpDynamic : AbstractIndexCreationTask<Listing>
    {
        public IndexCsharpDynamic()
        {
            Map = listings => from listing in listings select new { _ = CreateField("Seller", listing.Seller) };
        }
    }

    private class IndexJsStaticFields : AbstractJavaScriptIndexCreationTask
    {
        public IndexJsStaticFields()
        {
            Maps = new HashSet<string>
            {
                @"map('Listings', listing => {
    return {
        Seller: listing.Seller
    }
})",
            };
        }
    }
}

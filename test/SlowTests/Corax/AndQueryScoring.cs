using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Corax;

public class AndQueryScoring(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void AndGroupUnderOrIsScored()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        StoreAndIndex(store,
            new Product { Id = "products/1", Name = "laptop", Tags = ["sale"], Category = "electronics" },
            new Product { Id = "products/2", Name = "phone", Tags = ["new"], Category = "books" },
            new Product { Id = "products/3", Name = "tablet", Tags = ["new"], Category = "books" },
            new Product { Id = "products/4", Name = "e-reader", Tags = ["new"], Category = "books" });

        using var session = store.OpenSession();
        var results = Query(session, "where (Name = 'laptop' and Tags = 'sale') or Category = 'books' order by score()");

        Assert.Equal("products/1", results[0].Id);
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void EveryClauseOfAndContributesToScore()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax, includeScoresAndDistances: true));
        StoreAndIndex(store,
            new Product { Id = "products/1", Name = "laptop", Tags = ["sale", "sale", "sale"] },
            new Product { Id = "products/2", Name = "laptop", Tags = ["sale"] },
            new Product { Id = "products/3", Name = "phone", Tags = ["sale"] });

        using var session = store.OpenSession();
        var results = Query(session, "where Name = 'laptop' and Tags = 'sale' order by score()");
        var scores = results.ToDictionary(x => x.Id, x => (double)session.Advanced.GetMetadataFor(x)[Raven.Client.Constants.Documents.Metadata.IndexScore]);

        Assert.True(scores["products/1"] > scores["products/2"], $"products/1: {scores["products/1"]}, products/2: {scores["products/2"]}");
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying, Skip = "Corax 2.0 has not ported RavenDB-27606: an AND under an OR still adds its score to documents it rejects")]
    public void AndUnderOrReachedViaAndWithDoesNotScoreDocumentsItRejects()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax, includeScoresAndDistances: true));
        using (var bulk = store.BulkInsert())
        {
            bulk.Store(new Doc { A = "a1", B = "b1", C = "cx", D = "d1", E = "e1" }, "docs/T");
            bulk.Store(new Doc { A = "a1", B = "bx", C = "c1", D = "d1", E = "e1" }, "docs/L");
            bulk.Store(new Doc { A = "ax", B = "bx", C = "c1", D = "d1", E = "e1" }, "docs/K");
            bulk.Store(new Doc { A = "a1", B = "b1", C = "c1", D = "d1", E = "e1" }, "docs/X");
            for (var i = 0; i < 28; i++)
                bulk.Store(new Doc { A = "ax", B = "b1", C = "cx", D = "dx", E = "ex" });
            for (var i = 0; i < 3; i++)
                bulk.Store(new Doc { A = "ax", B = "bx", C = "c1", D = "dx", E = "ex" });
            for (var i = 0; i < 10_000; i++)
                bulk.Store(new Doc { A = "ax", B = "bx", C = "cx", D = "dx", E = "ex" });
        }

        new DocsIndex().Execute(store);
        Indexes.WaitForIndexing(store);

        using var session = store.OpenSession();
        var results = session.Advanced
            .RawQuery<Doc>($"from index '{new DocsIndex().IndexName}' where ((A = 'a1' and B = 'b1') or C = 'c1') and (D = 'd1' or E = 'e1') order by score()")
            .ToList();
        var scores = results.ToDictionary(x => x.Id, x => (double)session.Advanced.GetMetadataFor(x)[Raven.Client.Constants.Documents.Metadata.IndexScore]);

        // docs/L has A = 'a1' but fails (A and B), so it matches the same clauses as docs/K
        Assert.Equal(scores["docs/K"], scores["docs/L"], 4);
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying, Skip = "Corax 2.0 MoreLikeThis does not rank its results yet (RavenDB-27536)")]
    public void MoreLikeThisKeepsFilterBoostWithFieldOrderBy()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        using (var session = store.OpenSession())
        {
            session.Store(new Article { Category = "src", Title = "a", Body = "ravendb corax scoring engine ranking" }, "articles/1");
            session.Store(new Article { Category = "blog", Title = "b", Body = "ravendb corax scoring engine ranking fast" }, "articles/2");
            session.Store(new Article { Category = "news", Title = "c", Body = "ravendb corax" }, "articles/3");
            session.SaveChanges();
        }

        new Articles().Execute(store);
        Indexes.WaitForIndexing(store);

        using var s = store.OpenSession();
        const string where = "from index 'Articles' where morelikethis(id() = 'articles/1', '{\"Fields\":[\"Body\"],\"MinimumTermFrequency\":1,\"MinimumDocumentFrequency\":1}') and (boost(Category = 'news', 10) or Category = 'blog')";
        var noOrder = s.Advanced.RawQuery<Article>(where).NoCaching().ToList().Select(x => x.Id).ToList();
        var byTitle = s.Advanced.RawQuery<Article>(where + " order by Title").NoCaching().ToList().Select(x => x.Id).ToList();

        Assert.Equal("articles/3", noOrder.First());
        Assert.Equal(noOrder, byTitle);
    }

    private static List<Product> Query(IDocumentSession session, string where) =>
        session.Advanced
            .RawQuery<Product>($"from index '{new ProductsIndex().IndexName}' {where}")
            .ToList();

    private void StoreAndIndex(IDocumentStore store, params Product[] products)
    {
        using (var session = store.OpenSession())
        {
            foreach (var product in products)
                session.Store(product);
            session.SaveChanges();
        }

        new ProductsIndex().Execute(store);
        Indexes.WaitForIndexing(store);
    }

    private class ProductsIndex : AbstractIndexCreationTask<Product>
    {
        public ProductsIndex()
        {
            Map = products => from product in products
                select new { product.Name, product.Tags, product.Category };
        }
    }

    private class Product
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public string[] Tags { get; set; }
        public string Category { get; set; }
    }

    private class DocsIndex : AbstractIndexCreationTask<Doc>
    {
        public DocsIndex()
        {
            Map = docs => from doc in docs
                select new { doc.A, doc.B, doc.C, doc.D, doc.E };
        }
    }

    private class Doc
    {
        public string Id { get; set; }
        public string A { get; set; }
        public string B { get; set; }
        public string C { get; set; }
        public string D { get; set; }
        public string E { get; set; }
    }

    private class Articles : AbstractIndexCreationTask<Article>
    {
        public Articles()
        {
            Map = articles => from a in articles select new { a.Category, a.Body, a.Title };
            Index(x => x.Body, FieldIndexing.Search);
        }
    }

    private class Article
    {
        public string Id { get; set; }
        public string Category { get; set; }
        public string Body { get; set; }
        public string Title { get; set; }
    }
}

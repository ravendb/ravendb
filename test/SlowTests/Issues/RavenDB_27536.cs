using System;
using System.Linq;
using FastTests;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.MoreLikeThis;
using Raven.Client.Documents.Session;
using Raven.Server.Config;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_27536 : RavenTestBase
{
    public RavenDB_27536(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void MoreLikeThisRanksItsResults(Options options)
    {
        using var store = GetDocumentStore(options);

        store.ExecuteIndex(new Widgets_ByCategoryAndName());

        var target = new Widget { Key = Guid.NewGuid(), Category = "common", Name = "Alpha Bravo Charlie" };
        var duplicate = new Widget { Key = Guid.NewGuid(), Category = "common", Name = "Alpha Bravo Charlie" };

        using (var session = store.OpenSession())
        {
            session.Store(target);

            // the fillers share Category only; the duplicate also matches on Name
            for (int i = 0; i < 150; i++)
                session.Store(new Widget { Key = Guid.NewGuid(), Category = target.Category, Name = $"filler-{i}" });

            session.Store(duplicate);
            session.SaveChanges();
        }

        Indexes.WaitForIndexing(store);

        using var s = store.OpenSession();

        // before the fix the duplicate came back last, outside a page of 100 out of 151 matches
        var page = Similar<Widgets_ByCategoryAndName>(s, target, boost: false, take: 100);

        Assert.Equal(duplicate.Key, page[0].Key);
        Assert.DoesNotContain(page, x => x.Key == target.Key);
        Assert.Equal(duplicate.Key, Similar<Widgets_ByCategoryAndName>(s, target, boost: true, take: 100)[0].Key);

        if (options.SearchEngineMode == RavenSearchEngineMode.Corax)
        {
            // Corax budgets the sort for the rows the loop drops; Lucene bounds its collector by the page size and returns 99
            Assert.Equal(100, page.Length);
        }
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void MoreLikeThisFillsThePageOnAFanoutIndex(Options options)
    {
        using var store = GetDocumentStore(options);

        store.ExecuteIndex(new Widgets_ByTag());

        // three index entries per document: the sort budget has to be in entries, not documents
        var target = new Widget { Key = Guid.NewGuid(), Category = "common", Name = "Alpha", Tags = new[] { "x", "y", "z" } };

        using (var session = store.OpenSession())
        {
            session.Store(target);

            for (int i = 0; i < 40; i++)
                session.Store(new Widget { Key = Guid.NewGuid(), Category = target.Category, Name = $"filler-{i}", Tags = new[] { "x", "y", "z" } });

            session.SaveChanges();
        }

        Indexes.WaitForIndexing(store);

        using var s = store.OpenSession();

        var page = Similar<Widgets_ByTag>(s, target, boost: false, take: 10);

        Assert.Equal(page.Length, page.Select(x => x.Key).Distinct().Count()); // one row per document, not one per entry

        if (options.SearchEngineMode == RavenSearchEngineMode.Corax)
        {
            Assert.Equal(10, page.Length);

            // blacklisted by id, so the sibling entries stay out too; Lucene compares one entry id and returns the base document
            Assert.DoesNotContain(page, x => x.Key == target.Key);
        }
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void MoreLikeThisReportsTheScoreItRanksBy(Options options)
    {
        using var store = GetDocumentStore(options);

        store.ExecuteIndex(new Widgets_WithScore());

        var target = new Widget { Key = Guid.NewGuid(), Category = "common", Name = "Alpha Bravo Charlie" };

        using (var session = store.OpenSession())
        {
            session.Store(target);

            // only half the fillers share a term, so the shared terms keep a non-zero idf
            for (int i = 0; i < 5; i++)
                session.Store(new Widget { Key = Guid.NewGuid(), Category = target.Category, Name = $"Alpha filler-{i}" });

            for (int i = 0; i < 5; i++)
                session.Store(new Widget { Key = Guid.NewGuid(), Category = "rare", Name = $"Delta filler-{i}" });

            session.SaveChanges();
        }

        Indexes.WaitForIndexing(store);

        using var s = store.OpenSession();

        var page = Similar<Widgets_WithScore>(s, target, boost: false, take: 5);

        Assert.NotEmpty(page);

        foreach (var metadata in page.Select(x => s.Advanced.GetMetadataFor(x)))
            Assert.NotNull(metadata[Constants.Documents.Metadata.IndexScore]);

        Assert.True((double)s.Advanced.GetMetadataFor(page[0])[Constants.Documents.Metadata.IndexScore] > 0);
    }

    private static Widget[] Similar<TIndex>(IDocumentSession session, Widget target, bool boost, int take)
        where TIndex : AbstractIndexCreationTask, new()
    {
        return session.Advanced.DocumentQuery<Widget, TIndex>()
            .MoreLikeThis(b => b
                .UsingDocument(x => x.WhereEquals("Key", target.Key.ToString()))
                .WithOptions(new MoreLikeThisOptions
                {
                    Boost = boost,
                    MaximumQueryTerms = int.MaxValue,
                    MinimumDocumentFrequency = 0,
                    MaximumDocumentFrequencyPercentage = 100,
                    MinimumTermFrequency = 0,
                    MinimumWordLength = 0
                }))
            .Take(take)
            .ToArray();
    }

    private sealed class Widget
    {
        public string Id { get; set; }

        public Guid Key { get; set; }

        public string Category { get; set; }

        public string Name { get; set; }

        public string[] Tags { get; set; }
    }

    private sealed class Widgets_ByCategoryAndName : AbstractIndexCreationTask<Widget>
    {
        public Widgets_ByCategoryAndName()
        {
            Map = widgets => from w in widgets select new { w.Key, w.Category, w.Name };

            Store(w => w.Key, FieldStorage.Yes);
            Store(w => w.Category, FieldStorage.Yes);
            Store(w => w.Name, FieldStorage.Yes);
        }
    }

    private sealed class Widgets_WithScore : AbstractIndexCreationTask<Widget>
    {
        public Widgets_WithScore()
        {
            Map = widgets => from w in widgets select new { w.Key, w.Category, w.Name };

            Store(w => w.Key, FieldStorage.Yes);
            Store(w => w.Category, FieldStorage.Yes);
            Store(w => w.Name, FieldStorage.Yes);

            // Corax hides score metadata behind this opt-in, Lucene reports it either way
            Configuration = new IndexConfiguration { [RavenConfiguration.GetKey(x => x.Indexing.CoraxIncludeDocumentScore)] = "true" };
        }
    }

    private sealed class Widgets_ByTag : AbstractIndexCreationTask<Widget>
    {
        public Widgets_ByTag()
        {
            Map = widgets => from w in widgets
                             from tag in w.Tags
                             select new { w.Key, w.Category, w.Name, Tag = tag };

            Store(w => w.Key, FieldStorage.Yes);
            Store(w => w.Category, FieldStorage.Yes);
            Store(w => w.Name, FieldStorage.Yes);
        }
    }
}

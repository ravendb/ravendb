using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.MoreLikeThis;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_27559 : RavenTestBase
{
    public RavenDB_27559(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
    public void MoreLikeThisPagesDocumentsAndExcludesTheBaseDocument(Options options)
    {
        using var store = GetDocumentStore(options);

        store.ExecuteIndex(new Widgets_ByTag());

        var target = new Widget { Key = Guid.NewGuid(), Name = "Alpha", Tags = new[] { "x", "y", "z" } };

        using (var session = store.OpenSession())
        {
            session.Store(target);

            for (var i = 0; i < 40; i++)
                session.Store(new Widget { Key = Guid.NewGuid(), Name = $"filler-{i}", Tags = new[] { "x", "y", "z" } });

            session.SaveChanges();
        }

        Indexes.WaitForIndexing(store);

        using var session2 = store.OpenSession();

        var first = Similar(session2, target, skip: 0, take: 10);
        var second = Similar(session2, target, skip: 10, take: 10);
        var both = Similar(session2, target, skip: 0, take: 20);

        Assert.Equal(20, both.Length);
        Assert.DoesNotContain(both, x => x.Key == target.Key);
        Assert.Equal(both.Select(x => x.Key), first.Concat(second).Select(x => x.Key));
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void MoreLikeThisFromJsonOnAnEmptyIndexReturnsNothing(Options options)
    {
        using var store = GetDocumentStore(options);

        store.ExecuteIndex(new Widgets_ByTag());
        Indexes.WaitForIndexing(store);

        using var session = store.OpenSession();

        var rows = session.Advanced.DocumentQuery<Widget, Widgets_ByTag>()
            .MoreLikeThis(b => b.UsingDocument("{ \"Name\": \"alpha\" }"))
            .ToArray();

        Assert.Empty(rows);
    }

    private static Widget[] Similar(IDocumentSession session, Widget target, int skip, int take)
    {
        return session.Advanced.DocumentQuery<Widget, Widgets_ByTag>()
            .MoreLikeThis(b => b
                .UsingDocument(x => x.WhereEquals("Key", target.Key.ToString()))
                .WithOptions(new MoreLikeThisOptions
                {
                    MaximumQueryTerms = int.MaxValue,
                    MinimumDocumentFrequency = 0,
                    MaximumDocumentFrequencyPercentage = 100,
                    MinimumTermFrequency = 0,
                    MinimumWordLength = 0
                }))
            .Skip(skip)
            .Take(take)
            .ToArray();
    }

    private sealed class Widget
    {
        public string Id { get; set; }

        public Guid Key { get; set; }

        public string Name { get; set; }

        public string[] Tags { get; set; }
    }

    private sealed class Widgets_ByTag : AbstractIndexCreationTask<Widget>
    {
        public Widgets_ByTag()
        {
            Map = widgets => from w in widgets
                             from tag in w.Tags
                             select new { w.Key, w.Name, Tag = tag };

            Store(w => w.Key, FieldStorage.Yes);
            Store(w => w.Name, FieldStorage.Yes);
            Store("Tag", FieldStorage.Yes);
        }
    }
}

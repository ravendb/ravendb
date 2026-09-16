using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Commands;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Corax;

public class ExistsQueryScoring(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenFact(RavenTestCategory.Querying)]
    public void ExistsInsideBoostedOrGroupKeepsResultSetAndBoostOrdering()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        using (var session = store.OpenSession())
        {
            session.Store(new Item { Id = "items/searched", Title = "red apple", Content = "fruit" });
            session.Store(new Item { Id = "items/equal", Title = "apple", Content = "fruit" });
            session.Store(new Item { Id = "items/neither", Title = "banana", Content = "fruit" });
            session.SaveChanges();

            var viaExists = session.Advanced
                .RawQuery<Item>("from Items where Content = 'fruit' and (boost(search(Title, 'red'), 5) or boost(Title = 'apple', 10) or exists(Title)) order by score()")
                .WaitForNonStaleResults()
                .ToList();

            Assert.Equal(["items/equal", "items/searched", "items/neither"], viaExists.Select(i => i.Id));

            var viaTrue = session.Advanced
                .RawQuery<Item>("from Items where Content = 'fruit' and (boost(search(Title, 'red'), 5) or boost(Title = 'apple', 10) or true) order by score()")
                .WaitForNonStaleResults()
                .ToList();

            Assert.Equal(viaTrue.Select(i => i.Id), viaExists.Select(i => i.Id));

            var viaExistsMultiSort = session.Advanced
                .RawQuery<Item>("from Items where Content = 'fruit' and (boost(search(Title, 'red'), 5) or boost(Title = 'apple', 10) or exists(Title)) order by score(), Title")
                .WaitForNonStaleResults()
                .ToList();

            Assert.Equal(["items/equal", "items/searched", "items/neither"], viaExistsMultiSort.Select(i => i.Id));
        }
    }

    [RavenFact(RavenTestCategory.Querying)]
    public void BoostedExistsContributesConstantScoreScaledByBoost()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax, includeScoresAndDistances: true));
        StoreItem(store, "items/with", optional: "anything");
        StoreItem(store, "items/withNull", optional: null, storeNullValue: true);
        StoreItem(store, "items/without", optional: null);

        using (var session = store.OpenSession())
        {
            var results = session.Advanced
                .RawQuery<Item>("from Items where Content = 'fruit' and (boost(exists(Optional), 3) or true) order by score()")
                .WaitForNonStaleResults()
                .ToList();

            Assert.Equal(3, results.Count);
            Assert.Equal("items/without", results[^1].Id);

            var scores = results.ToDictionary(i => i.Id,
                i => (double)session.Advanced.GetMetadataFor(i)[Raven.Client.Constants.Documents.Metadata.IndexScore]);

            Assert.Equal(scores["items/with"], scores["items/withNull"], 4);
            Assert.Equal(3d, scores["items/with"] - scores["items/without"], 4);
        }
    }

    [RavenFact(RavenTestCategory.Querying)]
    public void MultiValuedExistsContributesConstantOncePerDocument()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax, includeScoresAndDistances: true));
        using (var session = store.OpenSession())
        {
            session.Store(new Item { Id = "items/many", Content = "fruit", Tags = ["a", "b", "c", "d", "e"] });
            session.Store(new Item { Id = "items/one", Content = "fruit", Tags = ["z"] });
            session.SaveChanges();
        }

        using (var session = store.OpenSession())
        {
            var results = session.Advanced
                .RawQuery<Item>("from Items where Content = 'fruit' and (boost(exists(Tags), 2) or true) order by score()")
                .WaitForNonStaleResults()
                .ToList();

            Assert.Equal(2, results.Count);

            var scores = results.ToDictionary(i => i.Id,
                i => (double)session.Advanced.GetMetadataFor(i)[Raven.Client.Constants.Documents.Metadata.IndexScore]);

            Assert.Equal(scores["items/one"], scores["items/many"], 4);
        }
    }

    [RavenFact(RavenTestCategory.Querying)]
    public void ExistsUnderScoringKeepsResultSetOfUnscoredExists()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        StoreItem(store, "items/1", optional: "x");
        StoreItem(store, "items/2", optional: "y");
        StoreItem(store, "items/3", optional: null);

        using (var session = store.OpenSession())
        {
            var unscored = session.Advanced
                .RawQuery<Item>("from Items where exists(Optional)")
                .WaitForNonStaleResults()
                .ToList();

            var scored = session.Advanced
                .RawQuery<Item>("from Items where exists(Optional) order by score()")
                .WaitForNonStaleResults()
                .ToList();

            Assert.Equal(new[] { "items/1", "items/2" }, unscored.Select(i => i.Id).OrderBy(i => i));
            Assert.Equal(unscored.Select(i => i.Id).OrderBy(i => i), scored.Select(i => i.Id).OrderBy(i => i));
        }
    }

    private static void StoreItem(IDocumentStore store, string id, string optional, bool storeNullValue = false)
    {
        var requestExecutor = store.GetRequestExecutor();
        using (requestExecutor.ContextPool.AllocateOperationContext(out var context))
        {
            var json = new DynamicJsonValue
            {
                ["@metadata"] = new DynamicJsonValue { ["@collection"] = "Items" },
                ["Content"] = "fruit"
            };
            if (optional != null)
                json["Optional"] = optional;
            else if (storeNullValue)
                json["Optional"] = null;

            var reader = context.ReadObject(json, id);
            requestExecutor.Execute(new PutDocumentCommand(requestExecutor.Conventions, id, null, reader), context);
        }
    }

    private class Item
    {
        public string Id { get; set; }
        public string Title { get; set; }
        public string Content { get; set; }
        public string Optional { get; set; }
        public string[] Tags { get; set; }
    }
}

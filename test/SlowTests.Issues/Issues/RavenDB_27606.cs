using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions;
using Raven.Server.Config;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_27606 : RavenTestBase
{
    public RavenDB_27606(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [InlineData("(Tag = $t1 and Kind = 'k1') or (Tag = $t2 and Kind = 'k2')")]
    [InlineData("(Tag = $t1 and (Kind = 'k1' or (Kind = 'k2' and Name = 'n'))) or (Tag = $t2 and (Kind = 'k2' or (Kind = 'k1' and Name = 'n')))")]
    public void GroupScoresDoNotDependOnCachedPlanOrder(string where)
    {
        var q = $"from index 'Items/ByAll' where {where} order by score()";

        Dictionary<string, double> warm, cold;
        using (var store = NewStore(SeedGroups))
        {
            Scores(store, q, ("t1", "r"), ("t2", "c"));
            warm = Scores(store, q, ("t1", "c"), ("t2", "r"));
        }

        using (var store = NewStore(SeedGroups))
            cold = Scores(store, q, ("t1", "c"), ("t2", "r"));

        AssertSameScores(cold, warm);
    }

    // A compound field (City, Age) and an ORDER BY on another field.
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [InlineData("boost(City = 'London', 2) and Age = 40 order by Name", new[] { "items/2", "items/3", "items/1" })]
    [InlineData("City = 'London' and Age = 40 order by Name", new[] { "items/2", "items/3", "items/1" })]
    [InlineData("City = 'London' and Age = 40 order by Name desc", new[] { "items/1", "items/3", "items/2" })]
    public void CompoundLookupHonoursOrderBy(string where, string[] expected)
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        using (var session = store.OpenSession())
        {
            session.Store(new Place { City = "London", Age = 40, Name = "c" }, "items/1");
            session.Store(new Place { City = "London", Age = 40, Name = "a" }, "items/2");
            session.Store(new Place { City = "London", Age = 40, Name = "b" }, "items/3");
            session.Store(new Place { City = "Paris", Age = 40, Name = "z" }, "items/4");
            session.Store(new Place { City = "London", Age = 30, Name = "y" }, "items/5");
            session.SaveChanges();
        }

        new Places_ByCityAndAge().Execute(store);
        Indexes.WaitForIndexing(store);

        using (var session = store.OpenSession())
        {
            var ids = session.Advanced.RawQuery<Place>($"from index 'Places/ByCityAndAge' where {where}").NoCaching().ToList().Select(x => x.Id).ToArray();
            Assert.Equal(expected, ids);
        }
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [InlineData("Tags all in ('a', 'b') or Name = 'x'")]
    [InlineData("(Tags = 'a' and Tags = 'b') or Name = 'x'")]
    public void AllInUnderOrScoresOnlyItsOwnMatches(string where)
    {
        using var store = NewStore(SeedAllIn);
        var scores = Scores(store, $"from index 'Items/ByAll' where {where} order by score()");

        // a/1 matches only Name = 'x' (it has 'a' but not 'b'), exactly like a/3
        Assert.Equal(scores["a/3"], scores["a/1"], 5);
    }

    // One wildcard in search() is ranked per term whatever the operator.
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [InlineData("search(Name, 'ap*')")]
    [InlineData("search(Name, 'ap*', and)")]
    public void SingleWildcardSearchIsRankedForBothOperators(string where)
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax, includeScoresAndDistances: true));
        using (var session = store.OpenSession())
        {
            for (int i = 1; i <= 3; i++)
                session.Store(new SearchDoc { Name = "apple" }, $"d/{i}");
            session.Store(new SearchDoc { Name = "apricot" }, "d/4");
            session.SaveChanges();
        }

        new SearchDocs_ByName().Execute(store);
        Indexes.WaitForIndexing(store);

        using var s = store.OpenSession();
        var results = s.Advanced.RawQuery<SearchDoc>($"from index 'SearchDocs/ByName' where {where} order by score()").NoCaching().ToList();
        Assert.Equal("d/4", results[0].Id);
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void AndSearchSumsTermScores()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax, includeScoresAndDistances: true));
        using (var session = store.OpenSession())
        {
            session.Store(new SearchDoc { Name = "apple banana" }, "d/1");
            session.Store(new SearchDoc { Name = "apple banana apple banana" }, "d/2");
            session.Store(new SearchDoc { Name = "apple" }, "d/3");
            for (int i = 0; i < 5; i++)
                session.Store(new SearchDoc { Name = "cherry" }, $"f/{i}");
            session.SaveChanges();
        }

        new SearchDocs_ByName().Execute(store);
        Indexes.WaitForIndexing(store);

        var and = Scores(store, "from index 'SearchDocs/ByName' where search(Name, 'apple banana', and) order by score()");
        var or = Scores(store, "from index 'SearchDocs/ByName' where search(Name, 'apple banana') order by score()");

        Assert.Equal(new[] { "d/1", "d/2" }, and.Keys.OrderBy(x => x));
        foreach (var (id, score) in and)
            Assert.Equal(or[id], score, 1e-6);
        Assert.True(and["d/2"] > and["d/1"]);

        var withOther = Scores(store, "from index 'SearchDocs/ByName' where search(Name, 'apple banana', and) or search(Name, 'apple') order by score()");
        var other = Scores(store, "from index 'SearchDocs/ByName' where search(Name, 'apple') order by score()");
        Assert.Equal(other["d/3"], withOther["d/3"], 1e-6);
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Vector | RavenTestCategory.Querying)]
    public void BoostedVectorInUnscoredQuery()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        using (var session = store.OpenSession())
        {
            for (var i = 0; i < 5; i++)
                session.Store(new VectorDoc { Text = "car", Counter0 = 1 });
            session.Store(new VectorDoc { Text = "bike", Counter0 = 2 });
            session.SaveChanges();
        }

        new VectorDocs_ByText().Execute(store);
        Indexes.WaitForIndexing(store);

        using var s = store.OpenSession();
        var results = s.Advanced.RawQuery<VectorDoc>("from index 'VectorDocs/ByText' where boost(vector.search(Text, 'car'), 2) and Counter0 = 1 order by Counter0").NoCaching().ToList();
        Assert.Equal(5, results.Count);
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Vector | RavenTestCategory.Querying)]
    [InlineData("boost(vector.search(Text, 'car'), 2)")]
    [InlineData("boost(vector.search(Text, 'car') and Counter0 = 1, 2)")]
    public void BoostedSingleVectorIsNotSupported(string where)
    {
        using var store = NewVectorStore();
        var e = Assert.Throws<RavenException>(() => Scores(store, $"from index 'VectorDocs/ByText' where {where} order by score()"));
        Assert.Contains("Boosting the VectorSearchMatch is not supported yet.", e.Message);
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Vector | RavenTestCategory.Querying)]
    public void BoostedMultiVectorMultipliesItsScore()
    {
        using var store = NewVectorStore();
        var plain = Scores(store, "from index 'VectorDocs/ByText' where vector.search(Text, $texts) order by score()", Texts);
        var boosted = Scores(store, "from index 'VectorDocs/ByText' where boost(vector.search(Text, $texts), 2) order by score()", Texts);

        Assert.Equal(plain.Keys.OrderBy(x => x), boosted.Keys.OrderBy(x => x));
        foreach (var (id, score) in plain)
            Assert.Equal(2 * score, boosted[id], 1e-5);
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Vector | RavenTestCategory.Querying)]
    public void BoostedOrDoesNotBoostItsSingleVector()
    {
        using var store = NewVectorStore();
        var plain = Scores(store, "from index 'VectorDocs/ByText' where vector.search(Text, 'car') or Counter0 = 3 order by score()");
        var boosted = Scores(store, "from index 'VectorDocs/ByText' where boost(vector.search(Text, 'car') or Counter0 = 3, 2) order by score()");

        Assert.Equal(plain["d/1"], boosted["d/1"], 1e-6); // matched by the vector only
        Assert.True(boosted["d/4"] > plain["d/4"]); // matched by Counter0 = 3 as well
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Vector | RavenTestCategory.Querying)]
    public void BoostedVectorInAndBoostsItsFilterOnce()
    {
        using var store = NewVectorStore();
        var group = Scores(store, "from index 'VectorDocs/ByText' where boost(vector.search(Text, $texts) and Counter0 = 1, 2) order by score()", Texts);
        var vector = Scores(store, "from index 'VectorDocs/ByText' where boost(vector.search(Text, $texts), 2) and Counter0 = 1 order by score()", Texts);

        AssertSameScores(vector, group);
    }

    [RavenFact(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    public void BoostedQueryNotOrderedByScoreReportsNoScore()
    {
        var options = Options.ForSearchEngine(RavenSearchEngineMode.Corax);
        options.ModifyDatabaseRecord += record =>
        {
            record.Settings[RavenConfiguration.GetKey(x => x.Indexing.OrderByScoreAutomaticallyWhenBoostingIsInvolved)] = false.ToString();
            record.Settings[RavenConfiguration.GetKey(x => x.Indexing.CoraxIncludeDocumentScore)] = true.ToString();
        };

        using var store = GetDocumentStore(options);
        using (var session = store.OpenSession())
        {
            session.Store(new Item { Name = "a" });
            session.Store(new Item { Name = "b" });
            session.SaveChanges();
        }

        new Items_ByAll().Execute(store);
        Indexes.WaitForIndexing(store);

        using (var session = store.OpenSession())
        {
            var scored = session.Advanced.RawQuery<Item>("from index 'Items/ByAll' where boost(Name = 'a', 10) or Name = 'b' order by score()").ToList();
            Assert.All(scored, x => Assert.True(session.Advanced.GetMetadataFor(x).ContainsKey(Raven.Client.Constants.Documents.Metadata.IndexScore)));
        }

        using (var session = store.OpenSession())
        {
            var results = session.Advanced.RawQuery<Item>("from index 'Items/ByAll' where boost(Name = 'a', 10) or Name = 'b'").ToList();
            Assert.Equal(2, results.Count);
            Assert.All(results, x => Assert.False(session.Advanced.GetMetadataFor(x).ContainsKey(Raven.Client.Constants.Documents.Metadata.IndexScore)));
        }
    }

    private static void SeedGroups(IDocumentStore store)
    {
        using var session = store.OpenSession();
        for (int i = 0; i < 4; i++)
            session.Store(new Item { Tag = "r", Kind = i % 2 == 0 ? "k1" : "k2", Name = i % 3 == 0 ? "n" : "m" }, $"r/{i}");
        for (int i = 0; i < 60; i++)
            session.Store(new Item { Tag = "c", Kind = i % 2 == 0 ? "k1" : "k2", Name = i % 3 == 0 ? "n" : "m" }, $"c/{i}");
        session.SaveChanges();
    }

    private static void SeedAllIn(IDocumentStore store)
    {
        using var session = store.OpenSession();
        session.Store(new Item { Tags = ["a"], Name = "x" }, "a/1");
        session.Store(new Item { Tags = ["a", "b"], Name = "y" }, "a/2");
        session.Store(new Item { Tags = ["c"], Name = "x" }, "a/3");
        for (int i = 0; i < 6; i++)
            session.Store(new Item { Tags = ["z", i % 2 == 0 ? "a" : "b"], Name = "w" }, $"f/{i}");
        session.SaveChanges();
    }

    private IDocumentStore NewStore(Action<IDocumentStore> seed)
    {
        var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax, includeScoresAndDistances: true));
        seed(store);
        new Items_ByAll().Execute(store);
        Indexes.WaitForIndexing(store);
        return store;
    }

    private static readonly (string Name, object Value)[] Texts = [("texts", new[] { "car", "pizza" })];

    private IDocumentStore NewVectorStore()
    {
        var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax, includeScoresAndDistances: true));
        using (var session = store.OpenSession())
        {
            session.Store(new VectorDoc { Text = "car", Counter0 = 1 }, "d/1");
            session.Store(new VectorDoc { Text = "automobile", Counter0 = 1 }, "d/2");
            session.Store(new VectorDoc { Text = "bike", Counter0 = 2 }, "d/3");
            session.Store(new VectorDoc { Text = "pizza", Counter0 = 3 }, "d/4");
            session.SaveChanges();
        }

        new VectorDocs_ByText().Execute(store);
        Indexes.WaitForIndexing(store);
        return store;
    }

    private static Dictionary<string, double> Scores(IDocumentStore store, string rql, params (string Name, object Value)[] parameters)
    {
        using var session = store.OpenSession();
        var query = session.Advanced.RawQuery<Item>(rql);
        foreach (var (name, value) in parameters)
            query = query.AddParameter(name, value);
        return query.ToList().ToDictionary(x => x.Id, x => Convert.ToDouble(session.Advanced.GetMetadataFor(x)[Raven.Client.Constants.Documents.Metadata.IndexScore]));
    }

    private static void AssertSameScores(Dictionary<string, double> expected, Dictionary<string, double> actual)
    {
        Assert.Equal(expected.Keys.OrderBy(x => x), actual.Keys.OrderBy(x => x));
        foreach (var (id, score) in expected)
            Assert.True(Math.Abs(score - actual[id]) < 1e-6, $"{id}: expected {score}, actual {actual[id]}");
    }

    private class Item
    {
        public string Id { get; set; }
        public string Tag { get; set; }
        public string Kind { get; set; }
        public string[] Tags { get; set; }
        public string Name { get; set; }
        public string Date { get; set; }
    }

    private class Items_ByAll : AbstractIndexCreationTask<Item>
    {
        public Items_ByAll()
        {
            Map = items => from i in items select new { i.Tag, i.Kind, i.Tags, i.Name, i.Date };
        }
    }

    private class Place
    {
        public string Id { get; set; }
        public string City { get; set; }
        public int Age { get; set; }
        public string Name { get; set; }
    }

    private class Places_ByCityAndAge : AbstractIndexCreationTask<Place>
    {
        public Places_ByCityAndAge()
        {
            Map = places => from p in places select new { p.City, p.Age, p.Name };
            CompoundField("City", "Age");
        }
    }

    private class SearchDoc
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }

    private class SearchDocs_ByName : AbstractIndexCreationTask<SearchDoc>
    {
        public SearchDocs_ByName()
        {
            Map = docs => from d in docs select new { d.Name };
            Index(x => x.Name, FieldIndexing.Search);
        }
    }

    private class VectorDoc
    {
        public string Id { get; set; }
        public string Text { get; set; }
        public int Counter0 { get; set; }
    }

    private class VectorDocs_ByText : AbstractIndexCreationTask<VectorDoc>
    {
        public VectorDocs_ByText()
        {
            Map = docs => from doc in docs select new { Text = CreateVector(doc.Text), doc.Counter0 };
        }
    }
}

using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Server.Documents.Indexes.Persistence.Corax;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Queries.Timings;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_27192 : RavenTestBase
{
    public RavenDB_27192(ITestOutputHelper output) : base(output)
    {
    }

    // ComputeEffectiveOrderBy elides an ORDER BY key on a field an equality names, but the equality pins only the
    // representation it matched: Num = 1 matches the long term that 1.1 / 1.5 / 1.8 share, so double ordering must run.
    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void OrderByMustSurviveAnEqualityThatPinsOnlyOneRepresentation(Options options)
    {
        using var store = GetDocumentStore(options);

        using (var session = store.OpenSession())
        {
            session.Store(new DoubleItem { Num = 1.1 });
            session.Store(new DoubleItem { Num = 1.8 });
            session.Store(new DoubleItem { Num = 1.5 });
            session.SaveChanges();
        }

        new DoubleItems_ByNum().Execute(store);
        Indexes.WaitForIndexing(store);

        using var read = store.OpenSession();

        Assert.Equal(new[] { 1.1, 1.5, 1.8 }, Nums(read, "where Num == $v order by Num as double", 1L));

        // implicit ordering, not in the ticket, breaks the same way
        Assert.Equal(new[] { 1.1, 1.5, 1.8 }, Nums(read, "where Num == $v order by Num", 1L));

        // control: ties on the pinned representation, so only the count is asserted
        Assert.Equal(3, Nums(read, "where Num == $v order by Num as long", 1L).Count);

        Assert.Equal(new[] { 1.5 }, Nums(read, "where Num == $v order by Num as double", 1.5));
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void AStringEqualityStillPinsTheValue(Options options)
    {
        using var store = GetDocumentStore(options);

        using (var session = store.OpenSession())
        {
            session.Store(new Movie { Status = "Released", Year = 2001 });
            session.Store(new Movie { Status = "Released", Year = 1999 });
            session.Store(new Movie { Status = "Planned", Year = 2030 });
            session.SaveChanges();
        }

        new Movies_ByStatusAndYear().Execute(store);
        Indexes.WaitForIndexing(store);

        using var read = store.OpenSession();

        var years = read.Advanced
            .RawQuery<Movie>("from index 'Movies/ByStatusAndYear' where Status = $s order by Status, Year")
            .AddParameter("s", "Released")
            .ToList()
            .Select(x => x.Year)
            .ToList();

        Assert.Equal(new[] { 1999, 2001 }, years);
    }

    // Asserts the plan, not the answer: with Status pinned the result is identical whether or not the elision happens.
    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Indexes)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void AParameterisedEqualityOnATextOnlyFieldStillElides(Options options)
    {
        using var store = GetDocumentStore(options);

        using (var session = store.OpenSession())
        {
            for (int i = 0; i < 200; i++)
                session.Store(new Movie { Status = i % 2 == 0 ? "Released" : "Planned", Year = 2000 + i });

            session.SaveChanges();
        }

        new Movies_ByStatusAndYear().Execute(store);
        Indexes.WaitForIndexing(store);

        using var read = store.OpenSession();

        read.Advanced
            .RawQuery<Movie>("from index 'Movies/ByStatusAndYear' where Year between 2000 and 2199 and Status = $s order by Status, Year as long limit 25 include timings()")
            .AddParameter("s", "Released")
            .Timings(out var timings)
            .ToList();

        // With Status elided, Year is the only sort key left; if the elision stops, Status leads the sort instead.
        var plan = Assert.IsType<QueryInspectionNode>(timings.QueryPlan);
        var ordering = OrderingFields(plan);
        Assert.NotEmpty(ordering);
        Assert.True(ordering.All(field => field == "Year"),
            $"the pinned leading key should still be elided, ordering came from [{string.Join(", ", ordering)}]: {Describe(plan)}");
    }

    // The elision reads index state, so a plan cached while the field was text-only must not survive the first number.
    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Indexes)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void TheElisionIsRePlannedWhenTheFieldGainsNumericTerms(Options options)
    {
        using var store = GetDocumentStore(options);

        using (var session = store.OpenSession())
        {
            session.Store(new MixedItem { Tag = "x" });
            session.SaveChanges();
        }

        new MixedItems_ByTag().Execute(store);
        Indexes.WaitForIndexing(store);

        // warms the plan cache while Tag is text-only
        using (var first = store.OpenSession())
            first.Advanced.RawQuery<MixedItem>("from index 'MixedItems/ByTag' where Tag = $v order by Tag as double")
                .AddParameter("v", "x")
                .ToList();

        using (var session = store.OpenSession())
        {
            session.Store(new MixedItem { Tag = 1.1 });
            session.Store(new MixedItem { Tag = 1.8 });
            session.Store(new MixedItem { Tag = 1.5 });
            session.SaveChanges();
        }

        Indexes.WaitForIndexing(store);

        using var read = store.OpenSession();

        var nums = read.Advanced
            .RawQuery<MixedItem>("from index 'MixedItems/ByTag' where Tag = $v order by Tag as double")
            .AddParameter("v", 1L)
            .ToList()
            .Select(x => double.Parse(x.Tag.ToString()))
            .ToList();

        Assert.Equal(new[] { 1.1, 1.5, 1.8 }, nums);
    }

    // Same for a CreateField-only field: absent from the index definition, so nothing there marks its first number.
    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Indexes)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void TheElisionIsRePlannedWhenADynamicFieldGainsNumericTerms(Options options)
    {
        using var store = GetDocumentStore(options);

        using (var session = store.OpenSession())
        {
            session.Store(new MixedItem { Tag = "x" });
            session.SaveChanges();
        }

        new MixedItems_ByDynamicTag().Execute(store);
        Indexes.WaitForIndexing(store);

        // warms the plan cache while Dyn is text-only
        using (var first = store.OpenSession())
            first.Advanced.RawQuery<MixedItem>("from index 'MixedItems/ByDynamicTag' where Dyn = $v order by Dyn as double")
                .AddParameter("v", "x")
                .ToList();

        using (var session = store.OpenSession())
        {
            session.Store(new MixedItem { Tag = 1.1 });
            session.Store(new MixedItem { Tag = 1.8 });
            session.Store(new MixedItem { Tag = 1.5 });
            session.SaveChanges();
        }

        Indexes.WaitForIndexing(store);

        using var read = store.OpenSession();

        var nums = read.Advanced
            .RawQuery<MixedItem>("from index 'MixedItems/ByDynamicTag' where Dyn = $v order by Dyn as double")
            .AddParameter("v", 1L)
            .ToList()
            .Select(x => double.Parse(x.Tag.ToString()))
            .ToList();

        Assert.Equal(new[] { 1.1, 1.5, 1.8 }, nums);
    }

    // The snapshot has to exist before any field holds a number - that is the transition it guards. This pins
    // only that a text-only index gets one at all, not which part of UpdateIndexCache delivers it.
    [RavenFact(RavenTestCategory.Querying | RavenTestCategory.Indexes | RavenTestCategory.Corax)]
    public async Task TheNumericTermsSnapshotIsPublishedEvenWhenNothingIsNumeric()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));

        using (var session = store.OpenSession())
        {
            session.Store(new MixedItem { Tag = "x" });
            session.SaveChanges();
        }

        var definition = new MixedItems_ByTag();
        definition.Execute(store);
        Indexes.WaitForIndexing(store);

        var database = await GetDatabase(store.Database);
        var persistence = Assert.IsType<CoraxIndexPersistence>(database.IndexStore.GetIndex(definition.IndexName).IndexPersistence);

        Assert.NotNull(persistence.FieldsWithNumericTerms);
        Assert.Empty(persistence.FieldsWithNumericTerms);
    }

    // A -L/-D lookup empties when its last posting list does, so the raw walk stops reporting the field - while
    // a reader whose transaction predates that commit still holds the numbers. The set therefore only grows.
    [RavenFact(RavenTestCategory.Querying | RavenTestCategory.Indexes | RavenTestCategory.Corax)]
    public async Task TheNumericTermsSnapshotKeepsAFieldThatLostItsNumbers()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));

        using (var session = store.OpenSession())
        {
            session.Store(new MixedItem { Tag = 1.5 }, "items/1");
            session.SaveChanges();
        }

        var definition = new MixedItems_ByTag();
        definition.Execute(store);
        Indexes.WaitForIndexing(store);

        var database = await GetDatabase(store.Database);
        var persistence = Assert.IsType<CoraxIndexPersistence>(database.IndexStore.GetIndex(definition.IndexName).IndexPersistence);
        Assert.Contains("Tag", persistence.FieldsWithNumericTerms);

        using (var session = store.OpenSession())
        {
            session.Delete("items/1");
            session.Store(new MixedItem { Tag = "x" }, "items/2");
            session.SaveChanges();
        }

        Indexes.WaitForIndexing(store);

        Assert.Contains("Tag", persistence.FieldsWithNumericTerms);
    }

    // Where the order comes from: a SortingMatch sorts on one field, a direct scan takes it from the tree it drives.
    // A per-execution cost gate picks between the two shapes, so the field carries the signal, not the strategy -
    // and the DecisionTrail lists a strategy whether it was accepted or rejected.
    private static List<string> OrderingFields(QueryInspectionNode node)
    {
        var fields = new List<string>();
        Collect(node, fields);
        return fields;

        static void Collect(QueryInspectionNode current, List<string> into)
        {
            if (current.Parameters != null)
            {
                if (current.Operation == "SortingMatch" && current.Parameters.TryGetValue("FieldName", out var sorted))
                    into.Add(sorted);

                if (current.Operation == "DirectScan" && current.Parameters.TryGetValue("DrivingTree", out var driving))
                    into.Add(driving);
            }

            if (current.Children == null)
                return;

            foreach (var child in current.Children)
                Collect(child, into);
        }
    }

    private static string Describe(QueryInspectionNode node)
    {
        var parts = new List<string>();
        Collect(node, parts);
        return string.Join(" -> ", parts);

        static void Collect(QueryInspectionNode current, List<string> into)
        {
            into.Add(current.Operation);

            if (current.Children == null)
                return;

            foreach (var child in current.Children)
                Collect(child, into);
        }
    }

    private class MixedItem
    {
        public string Id { get; set; }

        public object Tag { get; set; }
    }

    private class MixedItems_ByTag : AbstractIndexCreationTask<MixedItem>
    {
        public MixedItems_ByTag()
        {
            Map = items => from i in items
                           select new { i.Tag };
        }
    }

    private class MixedItems_ByDynamicTag : AbstractIndexCreationTask<MixedItem>
    {
        public MixedItems_ByDynamicTag()
        {
            Map = items => from i in items
                           select new { _ = CreateField("Dyn", i.Tag) };
        }
    }

    private static List<double> Nums(IDocumentSession session, string rql, object value)
    {
        return session.Advanced
            .RawQuery<DoubleItem>($"from index 'DoubleItems/ByNum' {rql}")
            .AddParameter("v", value)
            .ToList()
            .Select(x => x.Num)
            .ToList();
    }

    private class DoubleItem
    {
        public string Id { get; set; }

        public double Num { get; set; }
    }

    private class DoubleItems_ByNum : AbstractIndexCreationTask<DoubleItem>
    {
        public DoubleItems_ByNum()
        {
            Map = items => from i in items
                           select new { i.Num };
        }
    }

    private class Movie
    {
        public string Id { get; set; }

        public string Status { get; set; }

        public int Year { get; set; }
    }

    private class Movies_ByStatusAndYear : AbstractIndexCreationTask<Movie>
    {
        public Movies_ByStatusAndYear()
        {
            Map = movies => from m in movies
                            select new { m.Status, m.Year };
        }
    }
}

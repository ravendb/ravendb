using System.Collections.Generic;
using System.Linq;
using FastTests;
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

    // An equality pins only the representation it matched: a long equality matches the long term 1.1 / 1.5 / 1.8
    // share, so it does not pin the double the sort reads.
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

        // implicit ordering reads the text term, which a long equality does not pin either
        Assert.Equal(new[] { 1.1, 1.5, 1.8 }, Nums(read, "where Num == $v order by Num", 1L));

        // ties on the pinned representation, so only the count is asserted
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

    // The template picks its sorted scans assuming every pinned key elides. When one survives, the scan would walk
    // the tree of a field that is no longer the leading sort key, so those strategies have to stand down.
    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Indexes)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void ASurvivingKeyStandsDownTheScanChosenForTheElidedOrder(Options options)
    {
        using var store = GetDocumentStore(options);

        using (var session = store.OpenSession())
        {
            // Every Num is 1 as a long, and Other descends as Num ascends - a scan driven by Other cannot pass
            // for Num order. Enough rows with a small page for the scan to look cheaper than a bitmap.
            for (int i = 0; i < 200; i++)
                session.Store(new DoubleItem { Num = 1.0 + i / 1000.0, Other = 1.0 - i / 1000.0 });

            session.SaveChanges();
        }

        new DoubleItems_ByNumAndOther().Execute(store);
        Indexes.WaitForIndexing(store);

        using var read = store.OpenSession();

        var nums = read.Advanced
            .RawQuery<DoubleItem>("from index 'DoubleItems/ByNumAndOther' where Num == $v and Other > $lo order by Num as double, Other as double limit 25")
            .AddParameter("v", 1L)
            .AddParameter("lo", 0.5)
            .ToList()
            .Select(x => x.Num)
            .ToList();

        Assert.Equal(25, nums.Count);
        Assert.Equal(nums.OrderBy(x => x).ToList(), nums);
        Assert.Equal(1.0, nums[0]);
    }

    // The other direction: the equality matched the representation the sort reads, so the key really is pinned.
    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Indexes)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void AnEqualityThatPinsTheSortedRepresentationStillElides(Options options)
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

        var results = read.Advanced
            .RawQuery<DoubleItem>("from index 'DoubleItems/ByNum' where Num == $v order by Num as long include timings()")
            .AddParameter("v", 1L)
            .Timings(out var timings)
            .ToList();

        Assert.Equal(3, results.Count);

        var plan = Assert.IsType<QueryInspectionNode>(timings.QueryPlan);
        Assert.False(Contains(plan, "SortingMatch"),
            $"a long equality pins the long term the sort reads, so the key should be elided: {Describe(plan)}");
    }

    private static bool Contains(QueryInspectionNode node, string operation)
    {
        if (node.Operation == operation)
            return true;

        if (node.Children == null)
            return false;

        foreach (var child in node.Children)
        {
            if (Contains(child, operation))
                return true;
        }

        return false;
    }

    // Where the order comes from: a SortingMatch sorts on a field, a direct scan takes it from the tree it drives.
    // A cost gate picks between the two per execution, so the field carries the signal and the strategy name does not.
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

        public double Other { get; set; }
    }

    private class DoubleItems_ByNumAndOther : AbstractIndexCreationTask<DoubleItem>
    {
        public DoubleItems_ByNumAndOther()
        {
            Map = items => from i in items
                           select new { i.Num, i.Other };
        }
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

using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Queries.Timings;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_27807 : RavenTestBase
{
    public RavenDB_27807(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void NotOverWrapperMustNegateTheWholeExpression(Options options)
    {
        using var store = GetDocumentStore(options);
        using (var bulk = store.BulkInsert())
        {
            for (int i = 0; i < 100; i++)
                bulk.Store(new Doc { Name = $"n{i:D3}", Age = i, Tag = (i % 10).ToString() });
        }

        new Docs_Index().Execute(store);
        Indexes.WaitForIndexing(store);

        using var session = store.OpenSession();
        var results = session.Advanced.RawQuery<Doc>("from index 'Docs/Index' where Age > 5 and not exact(Tag = '1' and Name = 'n011')").ToList();

        Assert.Equal(93, results.Count);
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void NotOverWrappedNotEqualsMustMeanEquals(Options options)
    {
        using var store = GetDocumentStore(options);
        using (var session = store.OpenSession())
        {
            for (int i = 0; i < 20; i++)
                session.Store(new Doc { Name = $"n{i:D3}", Age = i });
            session.SaveChanges();
        }

        new Docs_Index().Execute(store);
        Indexes.WaitForIndexing(store);

        using var querySession = store.OpenSession();
        var results = querySession.Advanced.RawQuery<Doc>("from index 'Docs/Index' where Age < 10 and not exact(Name != 'n005')").ToList();

        Assert.Equal(new[] { "n005" }, results.Select(x => x.Name));
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void NegatedExactNotEqualsIsAnEquality(Options options)
    {
        using var store = GetDocumentStore(options);
        Seed(store);

        // not exact(F != v) is F = v: alone, inside a negated group (not A or F = v) and inside an OR group
        var tag3 = Enumerable.Range(0, 10).Select(i => $"items/{i * 10 + 3}").ToArray();
        foreach (var (where, expected, entryScan) in new (string, string[], bool)[]
                 {
                     ("where Age >= 0 and not exact(Tag != '3')", tag3, true),
                     ("where startsWith(Name, 'n0') and not (Age >= 0 and exact(Tag != '3'))", tag3, false),
                     ("where Age >= 0 and (Tag = '3' or not exact(Name != 'n014'))", [.. tag3, "items/14"], true)
                 })
        {
            RavenTestHelper.AssertAll(() => where,
                () => Assert.Equal(expected.Order(), RunRql(store, $"from index 'Items/ByAll' {where}").Ids.Order()),
                () => Assert.Equal(expected.Order(), RunRql(store, $"from index 'Items/ByAll' {where}", "BitmapPipeline").Ids.Order()));
            if (entryScan == false)
                continue;

            int gate = Enumerable.Range(0, 16).FirstOrDefault(g => Run(store, where, "rvn_corax_entry_scan", (long)g).EntryScanAt == g, -1);
            RavenTestHelper.AssertAll(() => where,
                () => Assert.True(gate >= 0, "the entry scan has no gate for this query"),
                () => Assert.Equal(expected.Order(), Run(store, where, "rvn_corax_entry_scan", (long)gate).Ids.Order()));
        }

        var walk = RunRql(store, "from index 'Items/ByAll' where Age >= 0 and (Tag = '3' or not exact(Name != 'n014')) order by Age as long", "FieldSortedScan");
        Assert.Equal("FieldSortedScan", walk.Hint);
        Assert.Equal(new[] { 3, 13, 14, 23, 33, 43, 53, 63, 73, 83, 93 }.Select(i => $"items/{i}"), walk.Ids);
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void NegatedWrapperOverAGroupNegatesTheGroup(Options options)
    {
        using var store = GetDocumentStore(options);
        Seed(store);

        // items/14 is the one document with Name n014 and Tag 4; every edge document has Tag 4
        var numbered = Enumerable.Range(0, 100).ToArray();
        var edges = Enumerable.Range(0, 10).Select(k => $"items/e{k}").ToArray();
        string[] ageAtLeast0 = [.. Items(numbered), "items/e2", "items/e3", "items/e4", "items/e5", "items/e7", "items/e8", "items/e9"];
        string[] allButItem14 = ageAtLeast0.Except(["items/14"]).ToArray();
        foreach (var (where, expected) in new (string, string[])[]
                 {
                     ("where Age >= 0 and not exact(Name = 'n014' and Tag = '4')", allButItem14),
                     ("where Age >= 0 and not boost(Name = 'n014' and Tag = '4', 2)", allButItem14),
                     ("where Age >= 0 and not when($on = true, Name = 'n014' and Tag = '4')", allButItem14),
                     ("where Age >= 0 and not exact(Name != 'n014' and Tag = '4')", [.. Items(numbered.Where(i => i % 10 != 4).ToArray()), "items/14"]),
                     ("where Tag = '3' or not exact(Name = 'n014' or Tag = '4')", Items(numbered.Where(i => i % 10 != 4).ToArray())),
                     ("where Tag = '3' or not exact(Name = 'n014' and Tag = '4')", [.. Items(numbered.Where(i => i != 14).ToArray()), .. edges]),
                     ("where Tag = '4' and (endsWith(Name, '7') or not exact(Age > 50 and Score > 20.5))",
                         [.. Items(4, 14, 24, 34, 44), "items/e0", "items/e1", "items/e2", "items/e3", "items/e6", "items/e7"])
                 })
        {
            RavenTestHelper.AssertAll(() => where,
                () => Assert.Equal(expected.Order(), Run(store, where, "on", true).Ids.Order()));
        }

        static string[] Items(params int[] ids) => ids.Select(i => $"items/{i}").ToArray();
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void WrapperMustKeepTheFoldedConstant(Options options)
    {
        using var store = GetDocumentStore(options);
        using (var session = store.OpenSession())
        {
            for (int i = 0; i < 20; i++)
                session.Store(new Doc { Age = i, Tag = "t" + (i % 5) });
            session.SaveChanges();
        }

        new Docs_Index().Execute(store);
        Indexes.WaitForIndexing(store);

        using var querySession = store.OpenSession();
        Assert.Equal(10, querySession.Advanced.RawQuery<Doc>("from index 'Docs/Index' where Age < 10 and exact(Tag = 't1' or true)").ToList().Count);
        Assert.Equal(3, querySession.Advanced.RawQuery<Doc>("from index 'Docs/Index' where Age < 3 or exact(Tag = 't1' and not true)").ToList().Count);
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void WrapperOverAGroupThatFoldsToAConstantActsAsTheConstant(Options options)
    {
        using var store = GetDocumentStore(options);
        using (var session = store.OpenSession())
        {
            for (int i = 0; i < 20; i++)
                session.Store(new Doc { Age = i, Tag = "t" + (i % 5) });
            session.SaveChanges();
        }

        new Docs_Index().Execute(store);
        Indexes.WaitForIndexing(store);

        // Tag t1 is on ages 1, 6, 11 and 16; a folded true leaves Age < 10 (10 rows), a folded false leaves Age < 3 (3 rows)
        var cases = new (string Where, int Expected)[]
        {
            ("Age < 10 and exact(Tag = 't1' or true)", 10),
            ("Age < 3 or exact(Tag = 't1' and not true)", 3),
            ("Age < 10 and boost(Tag = 't1' or true, 2)", 10),
            ("Age < 3 or boost(Tag = 't1' and not true, 2)", 3),
            ("Age < 10 and when($on = true, Tag = 't1' or true)", 10),
            ("Age < 3 or when($on = true, Tag = 't1' and not true)", 3),
            ("Age < 10 and not (Tag = 't1' and not true)", 10),
            ("Age < 3 or not (Tag = 't1' or true)", 3),
            ("Age < 10 and not exact(Tag = 't1' and not true)", 10),
            ("Age < 3 or (Age > 15 and exact(Tag = 't1' or true))", 7),
            ("Age < 10 and (Age < 3 or exact(Tag = 't1' and not true))", 3)
        };

        RavenTestHelper.AssertAll(cases.SelectMany(c => new[] { true, false }.Select(on => (Action)(() =>
        {
            using var session = store.OpenSession();
            int count = session.Advanced.RawQuery<Doc>($"from index 'Docs/Index' where {c.Where}").AddParameter("on", on).ToList().Count;
            Assert.True(c.Expected == count, $"{c.Where} with $on = {on}: expected {c.Expected}, got {count}");
        }))).ToArray());
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void WhenOverAGroupThatFoldsToAConstantVanishesWhenTheConditionIsOff(Options options)
    {
        using var store = GetDocumentStore(options);
        using (var session = store.OpenSession())
        {
            for (int i = 0; i < 20; i++)
                session.Store(new Doc { Age = i, Tag = "t" + (i % 5) });
            session.SaveChanges();
        }

        new Docs_Index().Execute(store);
        Indexes.WaitForIndexing(store);

        // Tag t1 is on ages 1, 6, 11 and 16, Tag t2 on ages 2, 7, 12 and 17; with $on = false when() drops its operand
        var cases = new (string Where, int On, int Off)[]
        {
            ("Age < 3 or when($on = true, Tag = 't1' or true)", 20, 3),
            ("Age < 10 and when($on = true, Tag = 't1' and not true)", 0, 10),
            ("when($on = true, Tag = 't1' and not true)", 0, 20),
            ("Age < 10 and when($on = true, true and not true)", 0, 10),
            ("Age < 10 and not when($on = true, Tag = 't1' or true)", 0, 10),
            ("Age < 3 or not when($on = true, Tag = 't1' and not true)", 20, 3),
            ("Age < 3 or not when($on = true, Tag = 't1' and Age > 5)", 17, 3),
            ("when($on = true, Tag = 't2') or not when($on = true, Tag = 't1' or true)", 4, 20),
            ("Age < 10 and exact(when($on = true, Tag = 't1' and not true))", 0, 10)
        };

        RavenTestHelper.AssertAll(cases.SelectMany(c => new[] { true, false }.Select(on => (Action)(() =>
        {
            using var session = store.OpenSession();
            int count = session.Advanced.RawQuery<Doc>($"from index 'Docs/Index' where {c.Where}").AddParameter("on", on).ToList().Count;
            int expected = on ? c.On : c.Off;
            Assert.True(expected == count, $"{c.Where} with $on = {on}: expected {expected}, got {count}");
        }))).ToArray());
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void AndWithATrueSideThatLeftClausesStaysAnAnd(Options options)
    {
        using var store = GetDocumentStore(options);
        using (var session = store.OpenSession())
        {
            for (int i = 0; i < 20; i++)
                session.Store(new Doc { Age = i, Tag = "t" + (i % 5) });
            session.SaveChanges();
        }

        new Docs_Index().Execute(store);
        Indexes.WaitForIndexing(store);

        // Tag t2 is on ages 2, 7, 12 and 17; every true side below folds to a group with MatchAll that is still AND-ed in
        var cases = new (string Where, int Expected)[]
        {
            ("(Tag = 't1' or true) and (Tag = 't2' or Age > 17)", 6),
            ("exact(Tag = 't1' or true) and (Tag = 't2' or Age > 17)", 6),
            ("when($on = true, Tag = 't1' or true) and (Tag = 't2' or Age > 17)", 6),
            ("Age < 3 or exact((Tag = 't1' or true) and Age > 5)", 17),
            ("Age < 3 or not ((Tag = 't1' and not true) or Age > 15)", 16),
            ("Age < 10 and exact((Tag = 't1' or true) and (Age < 3 or Age > 17))", 3)
        };

        RavenTestHelper.AssertAll(cases.SelectMany(c => new[] { true, false }.Select(on => (Action)(() =>
        {
            using var session = store.OpenSession();
            int count = session.Advanced.RawQuery<Doc>($"from index 'Docs/Index' where {c.Where}").AddParameter("on", on).ToList().Count;
            Assert.True(c.Expected == count, $"{c.Where} with $on = {on}: expected {c.Expected}, got {count}");
        }))).ToArray());
    }

    private void Seed(IDocumentStore store)
    {
        store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
        {
            Name = "Items/ByAll",
            Maps = { "from d in docs.Items select new { d.Name, d.Tag, d.Age, d.Score, d.Tags }" }
        }));

        using var commands = store.Commands();
        var meta = new Dictionary<string, object> { ["@collection"] = "Items" };
        for (int i = 0; i < 100; i++)
        {
            commands.Put($"items/{i}", null, new Dictionary<string, object>
            {
                ["Name"] = $"n{i:D3}",
                ["Tag"] = (i % 10).ToString(),
                ["Age"] = (long)i,
                ["Score"] = i + 0.5,
                ["Tags"] = new[] { (i % 10).ToString(), "x" + i % 3 }
            }, meta);
        }

        var edges = new (string Field, object Value)[] { ("Age", null), ("Age", "abc"), ("Score", null), ("Score", "abc"), ("Name", null), ("Tags", System.Array.Empty<string>()) };
        var missing = new[] { "Age", "Score", "Name", "Tags" };
        for (int k = 0; k < edges.Length + missing.Length; k++)
        {
            var doc = new Dictionary<string, object> { ["Name"] = $"n9{k}", ["Tag"] = "4", ["Age"] = 200L + k, ["Score"] = 60.5, ["Tags"] = new[] { "4", "x0" } };
            if (k < edges.Length)
                doc[edges[k].Field] = edges[k].Value;
            else
                doc.Remove(missing[k - edges.Length]);
            commands.Put($"items/e{k}", null, doc, meta);
        }

        Indexes.WaitForIndexing(store);
    }

    private static (string[] Ids, string Hint, int EntryScanAt) Run(IDocumentStore store, string where, string parameter, object value) =>
        RunRql(store, $"from index 'Items/ByAll' {where}", parameters: (parameter, value));

    private static (string[] Ids, string Hint, int EntryScanAt) RunRql(IDocumentStore store, string rql, string strategy = null, params (string Name, object Value)[] parameters)
    {
        using var session = store.OpenSession();
        var query = session.Advanced.RawQuery<Item>($"{rql} include timings()")
            .NoCaching()
            .Timings(out QueryTimings timings);
        if (strategy != null)
            query = query.AddParameter("rvn_corax_strategy", strategy);
        if (strategy == "BitmapPipeline")
            query = query.AddParameter("rvn_corax_entry_scan", -1L); // the reference checks no clause per document
        foreach (var (name, value) in parameters)
            query = query.AddParameter(name, value);

        var ids = query.ToList().Select(x => x.Id).ToArray();
        var plan = (QueryInspectionNode)timings.QueryPlan;
        string hint = null;
        Find(plan, "CompiledQuery")?.Parameters?.TryGetValue("OptimizationHint", out hint);
        int entryScanAt = -1;
        var entryScan = Find(plan, "EntryScan");
        if (entryScan?.Parameters != null && entryScan.Parameters.TryGetValue("Taken", out var taken) && taken == "True")
            int.TryParse(entryScan.Parameters["SwitchedAfterClauses"], out entryScanAt);
        return (ids, hint, entryScanAt);
    }

    private static QueryInspectionNode Find(QueryInspectionNode node, string operation)
    {
        if (node == null || node.Operation == operation)
            return node;
        return node.Children?.Select(c => Find(c, operation)).FirstOrDefault(x => x != null);
    }

    private class Item
    {
        public string Id { get; set; }
    }

    private class Doc
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public long Age { get; set; }
        public string Tag { get; set; }
    }

    private class Docs_Index : AbstractIndexCreationTask<Doc>
    {
        public Docs_Index()
        {
            Map = docs => from d in docs
                select new { d.Name, d.Age, d.Tag };
        }
    }
}

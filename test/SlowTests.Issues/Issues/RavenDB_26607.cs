using System;
using System.Linq;
using FastTests;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Queries.Suggestions;
using Raven.Client.Exceptions;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_26607 : RavenTestBase
{
    public RavenDB_26607(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void MatchingOrOrderingOnANonIndexedFieldIsRejected(Options options)
    {
        using var store = GetDocumentStore(options);

        store.ExecuteIndex(new TestIndex());

        using (var session = store.OpenSession())
        {
            session.Store(new Mitarbeiter { Id = "1", Name = "MA 1", Prop = "alpha" });
            session.Store(new Mitarbeiter { Id = "2", Name = "MA 2", Prop = "beta" });
            session.SaveChanges();
        }

        Indexes.WaitForIndexing(store);

        using var s = store.OpenSession();

        // both engines used to answer these without looking at the field, and disagreed:
        // on '!= null' Corax reported every document, Lucene none
        var cases = new Func<System.Collections.Generic.IEnumerable<TestIndex.Result>>[]
        {
            () => s.Query<TestIndex.Result, TestIndex>().Where(r => r.Prop == "alpha").ToList(),
            () => s.Query<TestIndex.Result, TestIndex>().Where(r => r.Prop != null).ToList(),
            () => s.Query<TestIndex.Result, TestIndex>().Where(r => r.Prop == null).ToList(),
            () => s.Query<TestIndex.Result, TestIndex>().OrderBy(r => r.Prop).ToList()
        };

        foreach (var run in cases)
        {
            var e = Assert.Throws<InvalidQueryException>(() => run());
            Assert.Contains(nameof(TestIndex.Result.Prop), e.Message);
        }

        // the field is still stored, so a projection keeps reading it back
        var projected = s.Query<TestIndex.Result, TestIndex>()
            .Where(r => r.Name == "MA 1")
            .Select(r => new { r.Name, r.Prop })
            .ToList();

        Assert.Equal("alpha", Assert.Single(projected).Prop);
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void EveryMatchingFormOnANonIndexedFieldIsRejected(Options options)
    {
        using var store = GetDocumentStore(options);

        store.ExecuteIndex(new TestIndex());
        Store(store);

        foreach (var rql in new[]
                 {
                     "from index 'TestIndex' where exists(Prop)",
                     "from index 'TestIndex' where Prop in ('alpha', 'beta')",
                     "from index 'TestIndex' where startsWith(Prop, 'al')",
                     "from index 'TestIndex' where search(Prop, 'alpha')",
                     "from index 'TestIndex' where Prop between 'a' and 'z'",
                     "from index 'TestIndex' where Name = 'MA 1' or Prop = 'beta'",
                     "from index 'TestIndex' order by Prop desc",
                     "from index 'TestIndex' order by Name, Prop"
                 })
        {
            using var session = store.OpenSession();

            var e = Assert.Throws<InvalidQueryException>(() => session.Advanced.RawQuery<TestIndex.Result>(rql).ToList());
            Assert.Contains("Prop", e.Message);
        }

        // ordering by something the index does carry is unaffected
        using (var session = store.OpenSession())
        {
            var names = session.Advanced.RawQuery<TestIndex.Result>("from index 'TestIndex' order by Name desc").ToList();
            Assert.Equal(new[] { "MA 2", "MA 1" }, names.Select(r => r.Name));
        }
    }

    // a JavaScript index declares its fields in the definition, so they are seen like any other index's
    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void MatchingOrOrderingOnANonIndexedFieldOfAJavaScriptIndexIsRejected(Options options)
    {
        using var store = GetDocumentStore(options);

        store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
        {
            Name = "JsIndex",
            Maps = { "map('Mitarbeiters', function (m) { return { Name: m.Name, Prop: m.Prop }; })" },
            Fields =
            {
                ["Prop"] = new IndexFieldOptions { Indexing = FieldIndexing.No, Storage = FieldStorage.Yes },
                ["Name"] = new IndexFieldOptions { Storage = FieldStorage.Yes }
            }
        }));

        Store(store);

        foreach (var rql in new[]
                 {
                     "from index 'JsIndex' where Prop = 'alpha'",
                     "from index 'JsIndex' order by Prop"
                 })
        {
            using var session = store.OpenSession();

            var e = Assert.Throws<InvalidQueryException>(() => session.Advanced.RawQuery<TestIndex.Result>(rql).ToList());
            Assert.Contains("Prop", e.Message);
        }

        using (var session = store.OpenSession())
        {
            var projected = session.Advanced
                .RawQuery<TestIndex.Result>("from index 'JsIndex' where Name = 'MA 1' select Name, Prop")
                .ToList();

            Assert.Equal("alpha", Assert.Single(projected).Prop);
        }
    }

    // __all_fields spreads the option to every field, and each one is rejected on its own
    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void MatchingOnAFieldOfAnIndexThatIndexesNothingIsRejected(Options options)
    {
        using var store = GetDocumentStore(options);

        store.ExecuteIndex(new NothingIndexedIndex());
        Store(store);

        foreach (var rql in new[]
                 {
                     "from index 'NothingIndexedIndex' where Prop = 'alpha'",
                     "from index 'NothingIndexedIndex' where Name = 'MA 1'",
                     "from index 'NothingIndexedIndex' order by Name"
                 })
        {
            using var session = store.OpenSession();

            Assert.Throws<InvalidQueryException>(() => session.Advanced.RawQuery<TestIndex.Result>(rql).ToList());
        }

        using (var session = store.OpenSession())
        {
            Assert.Equal(2, session.Advanced.RawQuery<TestIndex.Result>("from index 'NothingIndexedIndex'").ToList().Count);
        }
    }

    // KNOWN GAP, pinned so that closing it is deliberate: the indexing option of a dynamic field is persisted nowhere,
    // and GetFieldMetadata fabricates FieldIndexingMode.Normal for a field missing from the mapping. The server cannot
    // tell such a field from one that was never indexed, so the query is answered instead of rejected.
    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void ADynamicFieldWithIndexingNoIsNotRejectedYet(Options options)
    {
        using var store = GetDocumentStore(options);

        store.ExecuteIndex(new DynamicFieldIndex());

        store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
        {
            Name = "JsDynamicFieldIndex",
            Maps = { "map('Mitarbeiters', function (m) { return { Name: m.Name, _: createField('Dyn', m.Prop, { indexing: 'No', storage: true }) }; })" },
            Fields = { ["Name"] = new IndexFieldOptions { Storage = FieldStorage.Yes } }
        }));

        Store(store);

        foreach (var index in new[] { "DynamicFieldIndex", "JsDynamicFieldIndex" })
        {
            using var session = store.OpenSession();

            // the value is in the index - the projection reads it back
            var projected = session.Advanced
                .RawQuery<DynResult>($"from index '{index}' where Name = 'MA 1' select Name, Dyn")
                .ToList();

            Assert.Equal("alpha", Assert.Single(projected).Dyn);

            // ...but matching finds nothing and ordering is meaningless, with no error either way
            Assert.Empty(session.Advanced.RawQuery<DynResult>($"from index '{index}' where Dyn = 'alpha'").ToList());
            Assert.Equal(2, session.Advanced.RawQuery<DynResult>($"from index '{index}' order by Dyn").ToList().Count);
        }
    }

    // A facet counts documents per term, so a field without terms yields an empty facet - which reads as 'nothing
    // matches'. Facets enter through Index.FacetedQuery and their field comes from the select clause.
    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void AFacetOnANonIndexedFieldIsRejected(Options options)
    {
        using var store = GetDocumentStore(options);

        store.ExecuteIndex(new TestIndex());
        Store(store);

        using var session = store.OpenSession();

        var e = Assert.Throws<InvalidQueryException>(() => session.Advanced
            .RawQuery<TestIndex.Result>("from index 'TestIndex' select facet(Prop)")
            .ExecuteAggregation());

        Assert.Contains("Prop", e.Message);

        Assert.Throws<InvalidQueryException>(() => session.Advanced
            .RawQuery<TestIndex.Result>("from index 'TestIndex' where Name = 'MA 1' select facet(Prop)")
            .ExecuteAggregation());

        Assert.Throws<InvalidQueryException>(() => session.Advanced
            .RawQuery<TestIndex.Result>("from index 'TestIndex' select facet(Prop < 'm', Prop >= 'm')")
            .ExecuteAggregation());

        // a facet on a field the index does carry still works
        var byName = session.Advanced
            .RawQuery<TestIndex.Result>("from index 'TestIndex' select facet(Name)")
            .ExecuteAggregation();

        Assert.Equal(2, Assert.Single(byName).Value.Values.Count);
    }

    // sum/min/max/avg read the terms of the field they aggregate, not of the one the facet groups by
    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void AFacetAggregatingANonIndexedFieldIsRejected(Options options)
    {
        using var store = GetDocumentStore(options);

        store.ExecuteIndex(new TestIndex());
        Store(store);

        using var session = store.OpenSession();

        foreach (var rql in new[]
                 {
                     "from index 'TestIndex' select facet(Name, sum(Price))",
                     "from index 'TestIndex' select facet(Name, min(Price))",
                     "from index 'TestIndex' select facet(Name, max(Price))",
                     "from index 'TestIndex' select facet(Name, avg(Price))"
                 })
        {
            var e = Assert.Throws<InvalidQueryException>(() => session.Advanced.RawQuery<TestIndex.Result>(rql).ExecuteAggregation());
            Assert.Contains("Price", e.Message);
        }

        // aggregating a field the index does carry still works
        var bySalary = session.Advanced
            .RawQuery<TestIndex.Result>("from index 'TestIndex' select facet(Name, sum(Salary))")
            .ExecuteAggregation();

        Assert.Equal(2, Assert.Single(bySalary).Value.Values.Count);
    }

    // 'exact' rewrites the mode inside GetFieldMetadata, so the check there reads the binding, not the metadata
    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void AnExactMatchOnANonIndexedFieldIsRejected(Options options)
    {
        using var store = GetDocumentStore(options);

        store.ExecuteIndex(new TestIndex());
        Store(store);

        using var session = store.OpenSession();

        foreach (var rql in new[]
                 {
                     "from index 'TestIndex' where exact(Prop = 'alpha')",
                     "from index 'TestIndex' where exact(Prop in ('alpha'))",
                     "from index 'TestIndex' order by exact(Prop)"
                 })
        {
            var e = Assert.Throws<InvalidQueryException>(() => session.Advanced.RawQuery<TestIndex.Result>(rql).ToList());
            Assert.Contains("Prop", e.Message);
        }
    }

    private class DynResult
    {
        public string Name { get; set; }

        public string Dyn { get; set; }
    }

    private class DynamicFieldIndex : AbstractIndexCreationTask<Mitarbeiter>
    {
        public DynamicFieldIndex()
        {
            Map = mitarbeitende => from m in mitarbeitende
                                   select new
                                   {
                                       m.Name,
                                       _ = CreateField("Dyn", m.Prop, new CreateFieldOptions { Indexing = FieldIndexing.No, Storage = FieldStorage.Yes })
                                   };

            Store("Name", FieldStorage.Yes);
        }
    }

    // suggestions are a fourth entry point: the suggested field has its own check, its where clause had none
    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void AWhereClauseOfASuggestionQueryOnANonIndexedFieldIsRejected(Options options)
    {
        using var store = GetDocumentStore(options);

        store.ExecuteIndex(new SuggestingIndex());
        Store(store);

        using var session = store.OpenSession();

        var e = Assert.Throws<InvalidQueryException>(() => session.Advanced
            .RawQuery<TestIndex.Result>("from index 'SuggestingIndex' where Prop = 'alpha' select suggest(Name, 'MA')")
            .ToList());

        Assert.Contains("Prop", e.Message);

        // suggesting on a field the index does carry still works
        var suggestions = session.Query<TestIndex.Result, SuggestingIndex>()
            .SuggestUsing(b => b.ByField(x => x.Name, "MA"))
            .Execute();

        Assert.True(suggestions.ContainsKey("Name"));
    }

    private class SuggestingIndex : AbstractIndexCreationTask<Mitarbeiter, TestIndex.Result>
    {
        public SuggestingIndex()
        {
            Map = mitarbeitende => from m in mitarbeitende select new TestIndex.Result { Name = m.Name, Prop = m.Prop };

            StoreAllFields(FieldStorage.Yes);
            Index(r => r.Name, FieldIndexing.Search);
            Index(r => r.Prop, FieldIndexing.No);
            Suggestion(r => r.Name);
        }
    }

    private void Store(Raven.Client.Documents.IDocumentStore store)
    {
        using (var session = store.OpenSession())
        {
            session.Store(new Mitarbeiter { Id = "1", Name = "MA 1", Prop = "alpha", Price = 10, Salary = 100 });
            session.Store(new Mitarbeiter { Id = "2", Name = "MA 2", Prop = "beta", Price = 20, Salary = 200 });
            session.SaveChanges();
        }

        Indexes.WaitForIndexing(store);
    }

    private class NothingIndexedIndex : AbstractIndexCreationTask<Mitarbeiter>
    {
        public NothingIndexedIndex()
        {
            Map = mitarbeitende => from m in mitarbeitende select new { m.Name, m.Prop };

            Index(Constants.Documents.Indexing.Fields.AllFields, FieldIndexing.No);
            StoreAllFields(FieldStorage.Yes);
        }
    }

    private class TestIndex : AbstractIndexCreationTask<Mitarbeiter, TestIndex.Result>
    {
        public class Result
        {
            public string Name { get; set; }

            public string Prop { get; set; }

            public int Price { get; set; }

            public int Salary { get; set; }
        }

        public TestIndex()
        {
            Map = mitarbeitende => from mitarbeiter in mitarbeitende
                                   select new Result
                                   {
                                       Name = mitarbeiter.Name,
                                       Prop = mitarbeiter.Prop,
                                       Price = mitarbeiter.Price,
                                       Salary = mitarbeiter.Salary
                                   };

            StoreAllFields(FieldStorage.Yes);
            Index(r => r.Prop, FieldIndexing.No);
            Index(r => r.Price, FieldIndexing.No);
        }
    }

    private class Mitarbeiter
    {
        public string Id { get; set; }

        public string Name { get; set; }

        public string Prop { get; set; }

        public int Price { get; set; }

        public int Salary { get; set; }
    }
}

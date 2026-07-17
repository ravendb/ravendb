using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Facets;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Corax;

/// <summary>
/// RavenDB-27054: the Corax indexed term-facet path resolved the facet field from the display
/// name/alias (result.Key) instead of the actual indexed field (AggregateBy). A facet declared with
/// an alias (e.g. 'facet(BrandName as Merk)') therefore returned empty facet values — silently on
/// indexes with dynamic fields, or as a "Field not found" error otherwise — whenever it was reachable
/// via the indexed path. Regression exposed by RavenDB-26098, which routed WHERE-clause facet queries
/// through that path. Verifies correctness and Lucene parity.
/// </summary>
public class RavenDB_27054 : RavenTestBase
{
    public RavenDB_27054(ITestOutputHelper output) : base(output)
    {
    }

    private class Thing
    {
        public string Id { get; set; }
        public string Slug { get; set; } = "";
        public string BrandName { get; set; } = "";
        public Guid BrandId { get; set; }
        public List<string> CategoryNames { get; set; } = new();
        public int SortOrder { get; set; }
    }

    // Mirrors the reported production index: grouped map-reduce, dynamic CreateField sink,
    // Exact facet fields.
    private class Thing_FullProdShape : AbstractIndexCreationTask<Thing, Thing_FullProdShape.Result>
    {
        public class Result
        {
            public string Slug { get; set; } = "";
            public Guid BrandId { get; set; }
            public string BrandName { get; set; } = "";
            public IEnumerable<string> CategoryNames { get; set; } = Array.Empty<string>();
            public int OverviewOrder { get; set; }
            public object _ { get; set; }
        }

        public Thing_FullProdShape()
        {
            Map = things => from t in things
                            select new
                            {
                                t.Slug,
                                t.BrandId,
                                t.BrandName,
                                CategoryNames = t.CategoryNames.AsEnumerable(),
                                OverviewOrder = t.SortOrder,
                                _ = (object)null,
                            };

            Reduce = results => from r in results
                                group r by r.Slug into g
                                let first = g.OrderBy(x => x.OverviewOrder).First()
                                let extras = g.SelectMany(x => x.CategoryNames)
                                select new
                                {
                                    Slug = g.Key,
                                    first.BrandId,
                                    first.BrandName,
                                    first.CategoryNames,
                                    first.OverviewOrder,
                                    _ = extras.Select(v => CreateField("__extra", v, true, false)),
                                };

            Index(i => i.BrandId, FieldIndexing.Exact);
            Index(i => i.BrandName, FieldIndexing.Exact);
            Index(i => i.CategoryNames, FieldIndexing.Exact);
        }
    }

    private class Thing_Simple : AbstractIndexCreationTask<Thing>
    {
        public Thing_Simple()
        {
            Map = things => from t in things
                            select new { t.BrandName, t.BrandId, t.SortOrder };

            Index(i => i.BrandName, FieldIndexing.Exact);
            Index(i => i.BrandId, FieldIndexing.Exact);
        }
    }

    // Map-only index (no reduce) that still emits a dynamic CreateField sink, so it has the same
    // "output shape" as the production index minus the grouped reduce. Proves the grouped reduce is
    // not an ingredient: because the index has dynamic fields, an aliased facet fails silently
    // (empty values) instead of throwing.
    private class Thing_MapOnly_Dynamic : AbstractIndexCreationTask<Thing>
    {
        public Thing_MapOnly_Dynamic()
        {
            Map = things => from t in things
                            select new
                            {
                                t.BrandName,
                                t.BrandId,
                                CategoryNames = t.CategoryNames.AsEnumerable(),
                                _ = t.CategoryNames.Select(c => CreateField("__extra", c, true, false)),
                            };

            Index(i => i.BrandName, FieldIndexing.Exact);
            Index(i => i.BrandId, FieldIndexing.Exact);
            Index(i => i.CategoryNames, FieldIndexing.Exact);
        }
    }

    /// <summary>
    /// A term facet declared with an alias (<c>facet(BrandName as Merk)</c>) plus a WHERE clause
    /// must resolve the indexed field from AggregateBy, not from the alias. Regression: the reported
    /// production shape — grouped map-reduce with a dynamic CreateField sink and Exact facet fields —
    /// returned empty facet values (NRE server-side) on Corax after the WHERE-clause indexed path
    /// was enabled.
    /// </summary>
    [RavenTheory(RavenTestCategory.Facets)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.Single)]
    public void TermFacetsWithAliasAndWhere_FullProdShape(Options options)
    {
        using var store = GetDocumentStore(options);
        store.ExecuteIndex(new Thing_FullProdShape());

        var brandIds = SeedThings(store);
        Indexes.WaitForIndexing(store);

        using var session = store.OpenSession();

        var opts = new FacetOptions { PageSize = int.MaxValue, TermSortMode = FacetTermSortMode.ValueAsc, Start = 0 };
        var facets = new List<Facet>
        {
            new() { FieldName = nameof(Thing_FullProdShape.Result.BrandName),     DisplayFieldName = "Merk",     Options = opts },
            new() { FieldName = nameof(Thing_FullProdShape.Result.CategoryNames), DisplayFieldName = "Category", Options = opts },
        };

        var result = session.Advanced
            .DocumentQuery<Thing_FullProdShape.Result, Thing_FullProdShape>()
            .WhereIn(e => e.BrandId, brandIds.Cast<object>().ToArray())
            .OrderBy(e => e.OverviewOrder)
            .AggregateBy(facets)
            .Execute();

        Assert.True(result.ContainsKey("Merk"));
        Assert.True(result.ContainsKey("Category"));
        Assert.Equal(3, result["Merk"].Values.Count);
        Assert.Equal(30, result["Merk"].Values.Sum(v => v.Count));
        Assert.NotEmpty(result["Category"].Values);
    }

    /// <summary>
    /// Same alias bug on a plain map index (no dynamic fields): resolving the field from the alias
    /// used to throw "Field ... not found". Verifies the fix on the simplest shape.
    /// </summary>
    [RavenTheory(RavenTestCategory.Facets)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.Single)]
    public void TermFacetsWithAliasAndWhere_SimpleIndex(Options options)
    {
        using var store = GetDocumentStore(options);
        store.ExecuteIndex(new Thing_Simple());

        var brandIds = SeedThings(store);
        Indexes.WaitForIndexing(store);

        using var session = store.OpenSession();

        var result = session.Advanced
            .DocumentQuery<Thing, Thing_Simple>()
            .WhereIn(e => e.BrandId, brandIds.Cast<object>().ToArray())
            .AggregateBy(new Facet { FieldName = nameof(Thing.BrandName), DisplayFieldName = "Merk" })
            .Execute();

        Assert.True(result.ContainsKey("Merk"));
        Assert.Equal(3, result["Merk"].Values.Count);
        Assert.Equal(30, result["Merk"].Values.Sum(v => v.Count));
    }

    /// <summary>
    /// The alias bug also affected the WHERE-less indexed facet path. Verifies an aliased term facet
    /// with no WHERE clause returns values.
    /// </summary>
    [RavenTheory(RavenTestCategory.Facets)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.Single)]
    public void TermFacetsWithAliasNoWhere_SimpleIndex(Options options)
    {
        using var store = GetDocumentStore(options);
        store.ExecuteIndex(new Thing_Simple());

        SeedThings(store);
        Indexes.WaitForIndexing(store);

        using var session = store.OpenSession();

        var result = session.Advanced
            .DocumentQuery<Thing, Thing_Simple>()
            .AggregateBy(new Facet { FieldName = nameof(Thing.BrandName), DisplayFieldName = "Merk" })
            .Execute();

        Assert.True(result.ContainsKey("Merk"));
        Assert.Equal(3, result["Merk"].Values.Count);
        Assert.Equal(30, result["Merk"].Values.Sum(v => v.Count));
    }

    /// <summary>
    /// The nastiest variant: alias + no WHERE on an index WITH dynamic fields. Here the missing
    /// field does not throw (GetFieldMetadata builds a synthetic dynamic field), so the bug used to
    /// surface as silently empty facet values rather than an error. Mirrors the production index shape.
    /// </summary>
    [RavenTheory(RavenTestCategory.Facets)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.Single)]
    public void TermFacetsWithAliasNoWhere_FullProdShape(Options options)
    {
        using var store = GetDocumentStore(options);
        store.ExecuteIndex(new Thing_FullProdShape());

        SeedThings(store);
        Indexes.WaitForIndexing(store);

        using var session = store.OpenSession();

        var result = session.Advanced
            .DocumentQuery<Thing_FullProdShape.Result, Thing_FullProdShape>()
            .AggregateBy(new Facet { FieldName = nameof(Thing_FullProdShape.Result.BrandName), DisplayFieldName = "Merk" })
            .Execute();

        Assert.True(result.ContainsKey("Merk"));
        Assert.Equal(3, result["Merk"].Values.Count);
        Assert.Equal(30, result["Merk"].Values.Sum(v => v.Count));
    }

    /// <summary>
    /// Same alias + WHERE bug on a MAP-ONLY index (no reduce) that carries a dynamic CreateField sink.
    /// Confirms the grouped reduce is not required to trigger the bug, and that the presence of dynamic
    /// fields turns the failure into silently-empty facet values (mirrors Corax_Facets_MapOnly_SameOutputShape).
    /// </summary>
    [RavenTheory(RavenTestCategory.Facets)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.Single)]
    public void TermFacetsWithAliasAndWhere_MapOnlyWithDynamicField(Options options)
    {
        using var store = GetDocumentStore(options);
        store.ExecuteIndex(new Thing_MapOnly_Dynamic());

        var brandIds = SeedThings(store);
        Indexes.WaitForIndexing(store);

        using var session = store.OpenSession();

        var result = session.Advanced
            .DocumentQuery<Thing, Thing_MapOnly_Dynamic>()
            .WhereIn(e => e.BrandId, brandIds.Cast<object>().ToArray())
            .AggregateBy(new Facet { FieldName = nameof(Thing.BrandName), DisplayFieldName = "Merk" })
            .Execute();

        Assert.True(result.ContainsKey("Merk"));
        Assert.Equal(3, result["Merk"].Values.Count);
        Assert.Equal(30, result["Merk"].Values.Sum(v => v.Count));
    }

    /// <summary>
    /// Alias on a genuinely dynamic (CreateField-emitted) facet field, with a WHERE clause. This hits
    /// the dynamic branch of GetFieldMetadata rather than the static index-mapping branch the other
    /// alias tests exercise: the field must still be resolved from AggregateBy (the real dynamic field
    /// name '__extra'), not from the alias. Mirrors the production per-attribute dynamic facets.
    /// </summary>
    [RavenTheory(RavenTestCategory.Facets)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.Single)]
    public void TermFacetsWithAliasOnDynamicField_WithWhere(Options options)
    {
        using var store = GetDocumentStore(options);
        store.ExecuteIndex(new Thing_FullProdShape());

        var brandIds = SeedThings(store);
        Indexes.WaitForIndexing(store);

        using var session = store.OpenSession();

        var result = session.Advanced
            .DocumentQuery<Thing_FullProdShape.Result, Thing_FullProdShape>()
            .WhereIn(e => e.BrandId, brandIds.Cast<object>().ToArray())
            .OrderBy(e => e.OverviewOrder)
            // "__extra" is a dynamic field emitted by the reduce's CreateField sink; "Extra" is its alias.
            .AggregateBy(new Facet { FieldName = "__extra", DisplayFieldName = "Extra" })
            .Execute();

        Assert.True(result.ContainsKey("Extra"));
        Assert.NotEmpty(result["Extra"].Values);
        // Every doc carries the "Root" category, so it must appear with a positive count.
        Assert.Contains(result["Extra"].Values, v => v.Range == "Root" && v.Count > 0);
    }

    /// <summary>
    /// Sibling guard for the range-facet branch: range facets resolve the field from the parsed
    /// range expression (range.Field), not from result.Key, so an alias must not break them.
    /// Confirms the range path is unaffected by the term-facet alias bug.
    /// </summary>
    [RavenTheory(RavenTestCategory.Facets)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.Single)]
    public void RangeFacetsWithAliasAndWhere_SimpleIndex(Options options)
    {
        using var store = GetDocumentStore(options);
        store.ExecuteIndex(new Thing_Simple());

        var brandIds = SeedThings(store);
        Indexes.WaitForIndexing(store);

        using var session = store.OpenSession();

        var rangeFacet = new RangeFacet
        {
            Ranges = { "SortOrder < 10", "SortOrder >= 10 and SortOrder < 20", "SortOrder >= 20" },
            DisplayFieldName = "Positie"
        };

        var result = session.Advanced
            .DocumentQuery<Thing, Thing_Simple>()
            .WhereIn(e => e.BrandId, brandIds.Cast<object>().ToArray())
            .AggregateBy(rangeFacet)
            .Execute();

        Assert.True(result.ContainsKey("Positie"));
        Assert.Equal(3, result["Positie"].Values.Count);
        Assert.Equal(30, result["Positie"].Values.Sum(v => v.Count));
    }

    private static Guid[] SeedThings(IDocumentStore store)
    {
        var brands = new[] { "Acme", "Globex", "Initech" };
        var brandIds = new[] { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };
        var cats = new[] { "Root", "SubA", "SubB", "SubC" };
        using var session = store.OpenSession();
        for (int i = 0; i < 30; i++)
            session.Store(new Thing
            {
                Slug = $"s-{i}",
                BrandName = brands[i % brands.Length],
                BrandId = brandIds[i % brandIds.Length],
                CategoryNames = new List<string> { cats[0], cats[1 + (i % 3)] },
                SortOrder = i,
            });
        session.SaveChanges();
        return brandIds;
    }

}

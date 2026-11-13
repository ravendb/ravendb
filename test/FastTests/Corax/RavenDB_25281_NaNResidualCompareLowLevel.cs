using System;
using System.Text;
using System.Threading;
using Corax;
using Corax.Mappings;
using Corax.Querying;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using FastTests.Voron;
using Raven.Server.Documents.Indexes.Persistence.Corax;
using Raven.Server.Documents.Indexes.Persistence.Corax.QueryPlanBuilder;
using Raven.Server.Documents.Queries;
using Sparrow.Json;
using Sparrow.Server;
using Sparrow.Threading;
using Tests.Infrastructure;
using Voron;
using Xunit;
using IndexSearcher = Corax.Querying.IndexSearcher;
using IndexWriter = Corax.Indexing.IndexWriter;

namespace FastTests.Corax;

// RavenDB-25281: ResidualScanIlEmitter.EmitNumericCompareOp emits the double GreaterThanOrEqual /
// LessThanOrEqual cases as an ORDERED comparison (Clt/Cgt) followed by LogicalNot ("a >= b" as
// "!(a < b)"). Per ECMA-335, ordered IL comparisons involving NaN always evaluate to false, so for a
// NaN field value: Clt(NaN, v) = false --LogicalNot--> true, meaning NaN wrongly satisfies ">= v" (and
// symmetrically for Cgt/"<= v"). This is inconsistent with EmitDoubleBetween, which already uses the
// UNORDERED branch opcodes (Blt_Un/Bgt_Un) so BETWEEN correctly EXCLUDES NaN.
//
// This test writes a NaN double term directly through the low-level Corax IndexWriter API
// (entryWriter.Write(fieldId, textSpan, longValue, doubleValue)), bypassing the RavenDB document /
// auto-index field-conversion layer entirely. That bypass is required: RavenDB's blittable JSON writer
// (Sparrow/Json/BlittableWriter.cs) represents `double.NaN` as its STRING form ("NaN") rather than as an
// IEEE754 double, because standard/extended JSON has no native NaN literal. Consequently, when a document
// field is set to double.NaN, CoraxDocumentConverterBase.GetValueType sees a LazyStringValue at index time
// (not a LazyNumberValue) and indexes the field as TEXT, never reaching the numeric
// builder.Write(fieldId, ..., longValue, doubleValue) overload that ResidualScanIlEmitter's Double
// comparisons read from — so a NaN value can never reach the buggy comparison via a normal document/
// auto-index. That is a separate, pre-existing limitation of blittable JSON's NaN representation, not
// something this fix touches; the low-level IndexWriter API sidesteps it by writing the numeric term
// directly, exactly as test/FastTests/Corax/IndexSearcher.cs's IndexEntriesDouble helper does.
//
// The residual/entry-scan path (ResidualScanIlEmitter) only fires for a NON-seed clause in a multi-clause
// AND query (see BuildResolver.BuildScanPredicates: clause 0 is always the bitmap seed and never becomes
// a residual predicate), AND only for the clause that is not the AND-plan's cardinality-sorted driving
// clause (BuildResolver.cs sorts clauses by ascending estimated cardinality — the cheapest clause runs
// first). `Tag = 'rare'` (5 matching docs among hundreds of noise docs) is kept far cheaper than any
// `Content` range predicate so it reliably wins that sort and becomes clause 0/the seed; `Content`
// (double) then carries the GreaterThanOrEqual / LessThanOrEqual / Between comparisons under test as the
// residual predicate.
//
// QueryPlanBuilder.BuildFilterMatch (the low-level entry point IndexSearcher.cs's tests use) calls
// InstantiateBitmapPipeline directly and never wires CompiledQueryMatch.ForcedEntryScanGate from the
// $rvn_corax_entry_scan query parameter (only QueryPlanBuilder.Resolution.Instantiate.cs's Instantiate(),
// reachable only via BuildSortedQuery, does that — and BuildSortedQuery requires ORDER BY metadata this
// bare low-level harness cannot easily supply). Since ForcedEntryScanGate is a public field on
// CompiledQueryMatch, this test sets it directly on the match BuildFilterMatch returns instead of going
// through the query-parameter plumbing — functionally identical to what $rvn_corax_entry_scan does, just
// set from the test rather than parsed from RQL parameters.
public class RavenDB_25281_NaNResidualCompareLowLevel : StorageTest
{
    public RavenDB_25281_NaNResidualCompareLowLevel(ITestOutputHelper output) : base(output)
    {
    }

    private const int IdIndex = 0;
    private const int ContentIndex = 1;
    private const int TagIndex = 2;

    private sealed class Entry
    {
        public string Id;
        public double Content;
        public string Tag;
    }

    private static IndexFieldsMapping CreateKnownFields(ByteStringContext ctx)
    {
        Slice.From(ctx, "Id", ByteStringType.Immutable, out Slice idSlice);
        Slice.From(ctx, "Content", ByteStringType.Immutable, out Slice contentSlice);
        Slice.From(ctx, "Tag", ByteStringType.Immutable, out Slice tagSlice);

        using var builder = IndexFieldsMappingBuilder.CreateForWriter(false)
            .AddBinding(IdIndex, idSlice, null)
            .AddBinding(ContentIndex, contentSlice, null)
            .AddBinding(TagIndex, tagSlice, null);
        return builder.Build();
    }

    private void IndexEntries(Entry[] entries)
    {
        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
        using var mapping = CreateKnownFields(bsc);
        using var indexWriter = new IndexWriter(Env, mapping, SupportedFeatures.All);

        foreach (Entry entry in entries)
        {
            using var builder = indexWriter.Index(entry.Id);
            builder.Write(IdIndex, Encoding.UTF8.GetBytes(entry.Id));
            builder.Write(TagIndex, Encoding.UTF8.GetBytes(entry.Tag));

            double d = entry.Content;
            long longCompanion = double.IsNaN(d) ? 0L : Convert.ToInt64(d);
            string text = double.IsNaN(d) ? "NaN" : d.ToString(System.Globalization.CultureInfo.InvariantCulture);
            builder.Write(ContentIndex, Encoding.UTF8.GetBytes(text), longCompanion, d);

            builder.EndWriting();
        }

        indexWriter.Commit();
    }

    // Builds the match via QueryPlanBuilder.BuildFilterMatch, then sets ForcedEntryScanGate directly on
    // the returned CompiledQueryMatch (see the class-level comment for why: BuildFilterMatch's code path
    // never wires the $rvn_corax_entry_scan query parameter itself).
    private static IQueryMatch BuildQuery(IndexSearcher searcher, IndexFieldsMapping fields, ByteStringContext allocator, string rql, int forceEntryScanGate)
    {
        var queryMetadata = new QueryMetadata(rql, null, 0);
        var planParams = new PlanParameters
        {
            IndexSearcher = searcher,
            Metadata = queryMetadata,
            QueryParameters = null,
            Allocator = allocator
        };
        IQueryMatch match = QueryPlanBuilder.BuildFilterMatch(
            planParams,
            new QueryBuilderParameters(searcher, allocator, queryMetadata, null, fields),
            highlightingTerms: null,
            wantTimings: false,
            CancellationToken.None);

        if (match is CompiledQueryMatch cqm)
            cqm.ForcedEntryScanGate = forceEntryScanGate;

        return match;
    }

    private static System.Collections.Generic.List<long> Fill(IQueryMatch match)
    {
        var results = new System.Collections.Generic.List<long>();
        Span<long> buffer = stackalloc long[256];
        int count;
        while ((count = match.Fill(buffer)) > 0)
        {
            for (int i = 0; i < count; i++)
                results.Add(buffer[i]);
        }
        return results;
    }

    // "rare" is shared by exactly 5 documents; every other document carries a distinct common-N tag
    // and a Content value inside [50, 109] (so Content's range predicates alone match hundreds of docs —
    // much higher cardinality than Tag = 'rare'). The clause sorter in BuildResolver.cs orders AND clauses
    // by ascending estimated cardinality (see BuildResolver.cs: "sort executions by cardinality (smaller
    // clauses first)"), so Tag = 'rare' becomes clause 0 (the bitmap seed) and Content's comparison becomes
    // the residual predicate evaluated per-entry via ResidualScanIlEmitter — exactly the path under test.
    private const int NoiseDocCount = 500;

    private Entry[] SeedEntries()
    {
        var entries = new System.Collections.Generic.List<Entry>
        {
            new Entry { Id = "entry/nan", Content = double.NaN, Tag = "rare" },
            new Entry { Id = "entry/low", Content = 30.0, Tag = "rare" },   // below threshold
            new Entry { Id = "entry/high", Content = 70.0, Tag = "rare" },  // above threshold
            new Entry { Id = "entry/mid1", Content = 50.0, Tag = "rare" },  // at threshold
            new Entry { Id = "entry/mid2", Content = 50.0, Tag = "rare" },  // at threshold (duplicate value, distinct doc)
        };
        for (int i = 0; i < NoiseDocCount; i++)
        {
            entries.Add(new Entry { Id = $"entry/noise{i}", Content = 50.0 + (i % 60), Tag = $"common-{i % 50}" });
        }
        return entries.ToArray();
    }

    // Sweeps op-index gates (mirroring RavenDB_25281_BetweenOpenOpenScan.cs) until the entry-scan
    // residual path actually fires (CompiledQueryMatch.EntryScanTakenAtOp == the forced gate), and asserts
    // the residual result on every hit.
    private static void RunAcrossGates(IndexSearcher searcher, IndexFieldsMapping fields, ByteStringContext allocator, string rql, Action<System.Collections.Generic.List<string>> assertResult)
    {
        bool foundGate = false;
        for (int gate = 0; gate <= 15; gate++)
        {
            IQueryMatch match = BuildQuery(searcher, fields, allocator, rql, gate);
            var ids = Fill(match);
            var docIds = ResolveIds(searcher, ids);

            int takenAt = (match as CompiledQueryMatch)?.EntryScanTakenAtOp ?? -1;
            if (takenAt != gate)
                continue;

            foundGate = true;
            assertResult(docIds);
        }

        Assert.True(foundGate, "expected to find an entry-scan gate for the residual double comparison by sweeping op-indices 0..15");
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void GreaterThanOrEqual_ResidualScan_ExcludesNaN()
    {
        IndexEntries(SeedEntries());
        using var fields = CreateKnownFields(Allocator);
        using var searcher = new IndexSearcher(Env, fields);

        const string rql = "FROM TestIndex WHERE Tag = 'rare' AND Content >= 50.0";

        RunAcrossGates(searcher, fields, Allocator, rql, docIds =>
        {
            Assert.DoesNotContain("entry/nan", docIds);
            Assert.DoesNotContain("entry/low", docIds);
            Assert.Contains("entry/high", docIds);
            Assert.Contains("entry/mid1", docIds);
            Assert.Contains("entry/mid2", docIds);
            Assert.Equal(3, docIds.Count);
        });
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void LessThanOrEqual_ResidualScan_ExcludesNaN()
    {
        IndexEntries(SeedEntries());
        using var fields = CreateKnownFields(Allocator);
        using var searcher = new IndexSearcher(Env, fields);

        const string rql = "FROM TestIndex WHERE Tag = 'rare' AND Content <= 50.0";

        RunAcrossGates(searcher, fields, Allocator, rql, docIds =>
        {
            Assert.DoesNotContain("entry/nan", docIds);
            Assert.DoesNotContain("entry/high", docIds);
            Assert.Contains("entry/low", docIds);
            Assert.Contains("entry/mid1", docIds);
            Assert.Contains("entry/mid2", docIds);
            Assert.Equal(3, docIds.Count);
        });
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void Between_ResidualScan_ExcludesNaN()
    {
        // Cross-check: BETWEEN already uses the correct unordered branch opcodes (EmitDoubleBetween),
        // so this should pass both before and after the fix.
        IndexEntries(SeedEntries());
        using var fields = CreateKnownFields(Allocator);
        using var searcher = new IndexSearcher(Env, fields);

        const string rql = "FROM TestIndex WHERE Tag = 'rare' AND Content between 40.0 and 60.0";

        RunAcrossGates(searcher, fields, Allocator, rql, docIds =>
        {
            Assert.DoesNotContain("entry/nan", docIds);
            Assert.DoesNotContain("entry/low", docIds);
            Assert.DoesNotContain("entry/high", docIds);
            Assert.Contains("entry/mid1", docIds);
            Assert.Contains("entry/mid2", docIds);
            Assert.Equal(2, docIds.Count);
        });
    }

    private static System.Collections.Generic.List<string> ResolveIds(IndexSearcher searcher, System.Collections.Generic.List<long> entryIds)
    {
        var result = new System.Collections.Generic.List<string>(entryIds.Count);
        foreach (long id in entryIds)
            result.Add(searcher.TermsReaderFor("Id").GetTermFor(id));
        return result;
    }
}

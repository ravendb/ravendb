using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Corax;
using Corax.Mappings;
using Corax.Querying;
using Corax.Querying.Matches;
using Corax.Querying.Planning;
using Corax.Utils;
using FastTests.Voron;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Queries;
using Raven.Server.ServerWide;
using Sparrow.Server;
using Sparrow.Threading;
using Tests.Infrastructure;
using Voron;
using Xunit;
using IndexSearcher = Corax.Querying.IndexSearcher;
using IndexWriter = Corax.Indexing.IndexWriter;

namespace FastTests.Corax;

// RavenDB-25281: DirectScanFilteredMatch.Fill (the driving-tree walk backing a FieldSortedScan plan;
// src/Corax/Querying/Matches/DirectScanMatch.cs) had no cancellation check anywhere in its
// "while (count < remaining)" driving loop.
//
// This is tested by constructing the match directly (bypassing the RQL/QueryPlanBuilder/QueryRunner
// layers entirely, the same low-level style as RavenDB_25281_NaNResidualCompareLowLevel.cs) with an
// already-cancelled CancellationToken, then calling Fill() once and asserting OperationCanceledException.
//
// Why an already-cancelled token is safe to use here (unlike further up the query pipeline): every
// component this test's DirectScanFilteredMatch is built from - SortedDrivingMatch.Fill (the driving
// match) and the trivial pass-everything predicate delegate used here - has NO cancellation check of its
// own, pre- or post-fix. So DirectScanFilteredMatch's own loop is the ONLY place in this call graph that
// can throw OperationCanceledException; there is no earlier/other check to accidentally trip and produce
// a false-positive pass. (This is unlike the full CoraxIndexReadOperation/CompiledQueryMatch pipeline,
// where compiled-plan execution has its own pervasive, pre-existing IL-emitted cancellation checks that
// would fire first and mask whether this specific fix is present - which is why this test goes directly
// at the match rather than through a raw query.)
public class RavenDB_25281_ScanCancellation : StorageTest
{
    public RavenDB_25281_ScanCancellation(ITestOutputHelper output) : base(output)
    {
    }

    private const int IdIndex = 0;
    private const int SeqIndex = 1;

    private static IndexFieldsMapping CreateKnownFields(ByteStringContext ctx)
    {
        Slice.From(ctx, "Id", ByteStringType.Immutable, out Slice idSlice);
        Slice.From(ctx, "Seq", ByteStringType.Immutable, out Slice seqSlice);

        using var builder = IndexFieldsMappingBuilder.CreateForWriter(false)
            .AddBinding(IdIndex, idSlice, null)
            .AddBinding(SeqIndex, seqSlice, null);
        return builder.Build();
    }

    // Enough entries that a real (uncancelled) driving-tree walk would need several internal
    // ~EntryScanBatchSize(256)-sized batches to satisfy a large Take - large enough that "the loop never
    // even got a chance to run" is not a plausible explanation for a passing test.
    private const int EntryCount = 2000;

    private void IndexEntries(IndexFieldsMapping fields)
    {
        using var indexWriter = new IndexWriter(Env, fields, SupportedFeatures.All);
        for (int i = 0; i < EntryCount; i++)
        {
            string id = $"entry/{i}";
            using var builder = indexWriter.Index(id);
            builder.Write(IdIndex, Encoding.UTF8.GetBytes(id));
            builder.Write(SeqIndex, Encoding.UTF8.GetBytes(i.ToString()), i, i);
            builder.EndWriting();
        }

        indexWriter.Commit();
    }

    // Every entry passes: the predicate under test only needs to exist and be well-formed, DirectScan's
    // OWN driving loop (not the predicate) is what's being exercised for cancellation.
    private static int PassAllPredicate(QueryExecution exec, Span<EntryTermsReader> readers, Span<long> entryIds, Span<int> originalIndexes)
        => readers.Length;

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void DirectScanFilteredMatch_Fill_HonorsAlreadyCancelledToken()
    {
        using var fields = CreateKnownFields(Allocator);
        IndexEntries(fields);

        using var searcher = new IndexSearcher(Env, fields);
        Assert.True(fields.TryGetByFieldId(SeqIndex, out var seqBinding));

        // Driving provider: a full-range BETWEEN scan over Seq, in sort order - the same construction
        // ConstructDirectScan uses (see QueryPlanBuilder.Resolution.ConstructDirectScan.cs).
        var drivingProvider = searcher.BetweenQuery(seqBinding.Metadata, 0L, (long)(EntryCount - 1));
        Assert.IsType<TermsProviderMatch>(drivingProvider);
        var tpm = (TermsProviderMatch)drivingProvider;

        var cts = new CancellationTokenSource();
        cts.Cancel();

        var drivingMatch = new SortedDrivingMatch(tpm.Provider, tpm.Llt, Allocator, searcher, seqBinding.Metadata, nullFirst: false);
        var exec = new QueryExecution();

        // Take larger than one EntryScanBatchSize(256) batch so an uncancelled run would need multiple
        // iterations of the driving loop - the exact loop the fix adds a per-iteration check to.
        using var directScan = new DirectScanFilteredMatch(searcher, drivingMatch, exec, take: 1000, precompiledDelegate: PassAllPredicate, token: cts.Token);

        var matches = new long[1024];
        Assert.Throws<OperationCanceledException>(() => directScan.Fill(matches));
    }

    // Regression guard for the normal (uncancelled) path: the SAME construction, with a real, uncancelled
    // token, must still walk the driving tree and return matches - proving the fix's added check doesn't
    // regress ordinary DirectScan execution.
    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void DirectScanFilteredMatch_Fill_UncancelledTokenStillReturnsMatches()
    {
        using var fields = CreateKnownFields(Allocator);
        IndexEntries(fields);

        using var searcher = new IndexSearcher(Env, fields);
        Assert.True(fields.TryGetByFieldId(SeqIndex, out var seqBinding));

        var drivingProvider = searcher.BetweenQuery(seqBinding.Metadata, 0L, (long)(EntryCount - 1));
        Assert.IsType<TermsProviderMatch>(drivingProvider);
        var tpm = (TermsProviderMatch)drivingProvider;

        var drivingMatch = new SortedDrivingMatch(tpm.Provider, tpm.Llt, Allocator, searcher, seqBinding.Metadata, nullFirst: false);
        var exec = new QueryExecution();

        using var directScan = new DirectScanFilteredMatch(searcher, drivingMatch, exec, take: 1000, precompiledDelegate: PassAllPredicate, token: CancellationToken.None);

        Span<long> matches = stackalloc long[1024];
        int total = 0;
        int read;
        while ((read = directScan.Fill(matches)) > 0)
            total += read;

        Assert.Equal(1000, total);
    }
}

// RavenDB-25281: CoraxIndexReadOperation.QueryInternal's stats/count drain loop
// (src/Raven.Server/Documents/Indexes/Persistence/Corax/CoraxIndexReadOperation.cs, the
// "while(true) { ... Fill(ids) ... }" that runs AFTER the main results loop to compute an exact
// TotalResults) had no cancellation check at all.
//
// This is genuinely hard to isolate with an already-cancelled token: the very first Fill() call of any
// bitmap-pipeline query executes CompiledQueryMatch.Execute(), which runs emitted IL that has its OWN,
// pre-existing, pervasive ctx.Token.ThrowIfCancellationRequested() check (baked into nearly every
// compiled-plan op - see DualEmit.QueryILOps.cs IlCancellationCheck()). An already-cancelled token trips
// THAT check first regardless of whether the stats-loop fix exists, producing a false-positive pass.
//
// The trick used here to get a deterministic (non-timing-based) repro: QueryInternal is a C# iterator
// (`yield return` per result). Index.QueryInternal's *consumption* loop calls resultToFill.AddResultAsync
// once per yielded document, and for a streaming query that call reaches all the way out to our own
// IStreamQueryResultWriter.AddResultAsync callback - synchronously, on our own thread, BETWEEN successive
// MoveNext() calls on the iterator. A CancellationToken is a live view over its CancellationTokenSource
// (not a snapshot), so cancelling the source from inside that callback - after the single expected
// result (Limit=1) - lands strictly between "main loop finished, already executed the plan" and
// "control falls through to the stats-only drain loop": no race, no timing window, no dependence on
// dataset size.
public class RavenDB_25281_ScanCancellation_StatsLoop : RavenTestBase
{
    public RavenDB_25281_ScanCancellation_StatsLoop(ITestOutputHelper output) : base(output)
    {
    }

    private class Item
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public string Category { get; set; }
        public int Seq { get; set; }
    }

    private class Items_Index : AbstractIndexCreationTask<Item>
    {
        public Items_Index()
        {
            Map = items => from i in items
                select new { i.Name, i.Category, i.Seq };
        }
    }

    // Two non-trivial AND clauses (Seq range + Name equality) keep knownExactTotal at -1 (no O(1)
    // single-posting shortcut), so QueryInternal actually has to drain the stats loop to compute a total.
    private static List<Item> BuildSeed(int count)
    {
        var items = new List<Item>(count);
        for (int i = 0; i < count; i++)
        {
            items.Add(new Item
            {
                Id = $"items/{i}",
                Name = "Bob",
                Category = i % 2 == 0 ? "red" : "blue",
                Seq = i
            });
        }

        return items;
    }

    private static async Task SeedAsync(IDocumentStore store, List<Item> items)
    {
        using var bulk = store.BulkInsert();
        foreach (var it in items)
            await bulk.StoreAsync(it, it.Id);
    }

    /// <summary>Cancels <paramref name="cts"/> as soon as the expected number of results have been streamed back -
    /// i.e. exactly between the main results loop finishing and QueryInternal falling through to the stats drain loop.</summary>
    private sealed class CancelAfterNResultsWriter : IStreamQueryResultWriter<Document>
    {
        private readonly CancellationTokenSource _cts;
        private readonly int _cancelAfter;
        private int _seen;

        public CancelAfterNResultsWriter(CancellationTokenSource cts, int cancelAfter)
        {
            _cts = cts;
            _cancelAfter = cancelAfter;
        }

        public void StartResponse()
        {
        }

        public void StartResults()
        {
        }

        public void EndResults()
        {
        }

        public ValueTask AddResultAsync(Document res, CancellationToken token)
        {
            res.Dispose();
            _seen++;
            if (_seen >= _cancelAfter)
                _cts.Cancel();
            return ValueTask.CompletedTask;
        }

        public void EndResponse()
        {
        }

        public ValueTask WriteErrorAsync(Exception e) => ValueTask.CompletedTask;

        public ValueTask WriteErrorAsync(string error) => ValueTask.CompletedTask;

        public void WriteQueryStatistics(long resultEtag, bool isStale, string indexName, long totalResults, DateTime timestamp)
        {
        }

        public bool SupportStatistics => false;

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public async Task StatsCountLoop_HonorsTokenCancelledAfterMainLoopCompletes()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        var index = new Items_Index();
        index.Execute(store);
        var items = BuildSeed(4000);
        await SeedAsync(store, items);
        Indexes.WaitForIndexing(store);

        var dbInstance = await GetDocumentDatabaseInstanceFor(store, store.Database);

        // Limit=1 (and default SkipStatistics==false): the main results loop yields exactly one document
        // and stops: control falls through to the stats-only drain loop to compute the real TotalResults
        // over the remaining ~3999 matches - the loop this fix targets.
        var query = new IndexQueryServerSide($"from index '{index.IndexName}' where Seq between 0 and 3999 and Name = 'Bob'")
        {
            Limit = 1,
            PageSize = 1
        };
        Assert.False(query.SkipStatistics);

        using var context = QueryOperationContext.ShortTermSingleUse(dbInstance);
        using var cts = new CancellationTokenSource();
        using var token = new OperationCancelToken(cts.Token);
        var writer = new CancelAfterNResultsWriter(cts, cancelAfter: 1);
        var response = new DefaultHttpContext().Response;

        await Assert.ThrowsAsync<OperationCanceledException>(async () =>
            await dbInstance.QueryRunner.ExecuteStreamQuery(query, context, response, writer, token));
    }

    // Regression guard for the normal (uncancelled) path: the identical query/writer shape, but the
    // token is never cancelled - the stats loop must run to completion and report the true total.
    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public async Task StatsCountLoop_UncancelledTokenStillCompletes()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        var index = new Items_Index();
        index.Execute(store);
        var items = BuildSeed(4000);
        await SeedAsync(store, items);
        Indexes.WaitForIndexing(store);

        var dbInstance = await GetDocumentDatabaseInstanceFor(store, store.Database);

        var query = new IndexQueryServerSide($"from index '{index.IndexName}' where Seq between 0 and 3999 and Name = 'Bob'")
        {
            Limit = 1,
            PageSize = 1
        };

        using var context = QueryOperationContext.ShortTermSingleUse(dbInstance);
        using var cts = new CancellationTokenSource();
        using var token = new OperationCancelToken(cts.Token);
        // cancelAfter far beyond the single result this query yields: never actually cancels.
        var writer = new CancelAfterNResultsWriter(cts, cancelAfter: int.MaxValue);
        var response = new DefaultHttpContext().Response;

        await dbInstance.QueryRunner.ExecuteStreamQuery(query, context, response, writer, token);
    }
}

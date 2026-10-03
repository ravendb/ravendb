using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using BenchmarkDotNet.Attributes;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Queries.Facets;
using Raven.Client.Documents.Session;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using Tests.Infrastructure;
using Xunit.Abstractions;

namespace HighCardinalityFacets.Benchmark;

/// <summary>
/// A term facet over a field with 250k distinct values, with and without an aggregation, on both engines, for where
/// clauses matching a few hundred, 50k and 200k documents. A term facet used to cost a full pass over the facet field's
/// terms on every query, however few documents matched, so the facet dwarfed the query itself on high cardinality fields
/// such as ids. Walking the matches instead must not trade that time for memory when many documents match, which is what
/// the larger match counts and the memory diagnoser are for. One database is seeded once for the whole run and indexed
/// by both engines, each through its own index.
/// </summary>
[MemoryDiagnoser]
public class HighCardinalityFacetsBench
{
    private Harness _harness;

    [Params(RavenSearchEngineMode.Corax, RavenSearchEngineMode.Lucene)]
    public RavenSearchEngineMode Engine { get; set; }

    [Params(1_000_000)]
    public int Documents { get; set; }

    [Params(Harness.FewMatches, 50_000, 200_000)]
    public int Matches { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _harness = Harness.Shared(Documents);
        _harness.AssertMatches(Engine, Matches);
    }

    [Benchmark]
    public int CountByMerchant() => _harness.Facet(Engine, Matches, withSum: false);

    [Benchmark]
    public int SumAmountByMerchant() => _harness.Facet(Engine, Matches, withSum: true);
}

public class Harness : RavenTestBase
{
    // what the production shape of the where clause matches: a handful of customers over a date range and an entity type
    public const int FewMatches = 721;

    private const int MerchantCount = 250_000;
    private const int CustomerCount = 200;
    private const int ProgressEvery = 1_000_000;
    private static readonly TimeSpan IndexingTimeout = TimeSpan.FromHours(1);

    private static readonly DateTime Base = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime From = Base.AddDays(30);
    private static readonly DateTime To = Base.AddDays(90).AddSeconds(30);
    private static readonly string[] CustomerIds = Enumerable.Range(0, 5).Select(i => $"customers/{i}").ToArray();
    private static readonly string[] EntityTypes = { "Deposit" };

    // a small page keeps the response, and what the client has to read, the same whatever the match count
    private static readonly FacetOptions FirstPage = new FacetOptions { PageSize = 10 };

    // ConsoleTestOutputHelper marks the RavenTestBase instance as running outside of xUnit
    private static readonly ConsoleTestOutputHelper TestOutputHelper = new ConsoleTestOutputHelper();
    private static readonly object SharedLock = new object();
    private static Harness _shared;

    private IDocumentStore _store;
    private int _documents;

    private Harness(ITestOutputHelper output) : base(output)
    {
    }

    // BenchmarkDotNet runs GlobalSetup once per benchmark method and parameter set; seeding is shared across all of them
    public static Harness Shared(int documents)
    {
        lock (SharedLock)
        {
            if (_shared == null)
            {
                var harness = new Harness(TestOutputHelper);
                harness.Initialize(documents);
                _shared = harness;
            }

            return _shared;
        }
    }

    public static void DisposeShared()
    {
        lock (SharedLock)
        {
            _shared?.Dispose();
            _shared = null;
        }
    }

    private void Initialize(int documents)
    {
        var total = Stopwatch.StartNew();

        _documents = documents;

        // persist to disk so larger document counts do not have to fit the in-memory pager
        var options = new Options { RunInMemory = false };
        _store = GetDocumentStore(options);

        new CoinIndex(RavenSearchEngineMode.Corax).Execute(_store);
        new CoinIndex(RavenSearchEngineMode.Lucene).Execute(_store);

        Console.WriteLine($"[SETUP] seeding {documents:N0} documents into '{_store.Database}' at {DateTime.Now:HH:mm:ss}");

        var entityTypes = new[] { "Deposit", "Withdrawal", "Transfer" };
        var seeding = Stopwatch.StartNew();
        using (var bulk = _store.BulkInsert())
        {
            for (var i = 0; i < documents; i++)
            {
                bulk.Store(new Coin
                {
                    Id = $"coins/{i}",
                    CustomerId = $"customers/{i % CustomerCount}",
                    MerchantId = $"merchants/{i % MerchantCount}",
                    EntityType = entityTypes[i % 3],
                    CreatedAt = Base.AddMinutes(i),
                    Amount = i % 100
                });

                if ((i + 1) % ProgressEvery == 0)
                    Console.WriteLine($"[SETUP] stored {i + 1:N0} documents, {seeding.Elapsed:hh\\:mm\\:ss} elapsed, {(i + 1) / seeding.Elapsed.TotalSeconds:N0} docs/s");
            }
        }

        Console.WriteLine($"[SETUP] seeding done in {seeding.Elapsed:hh\\:mm\\:ss}, waiting for both indexes");

        WaitForIndexesWithProgress();

        OptimizeLuceneIndex();

        var coraxBuckets = Facet(RavenSearchEngineMode.Corax, FewMatches, withSum: true);
        var luceneBuckets = Facet(RavenSearchEngineMode.Lucene, FewMatches, withSum: true);
        if (coraxBuckets == 0 || coraxBuckets != luceneBuckets)
            throw new InvalidOperationException($"The facet returned {coraxBuckets} buckets on Corax and {luceneBuckets} on Lucene.");

        Console.WriteLine($"[SETUP] documents={documents:N0} facet buckets={coraxBuckets}");
        Console.WriteLine($"[SETUP] ready in {total.Elapsed:hh\\:mm\\:ss}");
    }

    private void WaitForIndexesWithProgress()
    {
        var indexing = Stopwatch.StartNew();
        while (true)
        {
            var stats = _store.Maintenance.Send(new GetIndexesStatisticsOperation());
            var report = string.Join(", ", stats.Select(s => $"{s.Name}: {s.EntriesCount:N0} entries{(s.IsStale ? ", stale" : string.Empty)}"));
            Console.WriteLine($"[SETUP] indexing {indexing.Elapsed:hh\\:mm\\:ss}: {report}");

            if (stats.All(s => s.IsStale == false))
                return;

            if (indexing.Elapsed > IndexingTimeout)
                throw new TimeoutException($"Indexing did not finish within {IndexingTimeout}: {report}");

            Thread.Sleep(TimeSpan.FromSeconds(30));
        }
    }

    // Lucene picks the facet path per segment and background merges leave a different segment layout on every run,
    // so the index is merged into one segment; Corax has no segments
    private void OptimizeLuceneIndex()
    {
        var database = Databases.GetDocumentDatabaseInstanceFor(_store).GetAwaiter().GetResult();
        var index = database.IndexStore.GetIndex(CoinIndex.NameFor(RavenSearchEngineMode.Lucene));

        var optimizing = Stopwatch.StartNew();
        index.Optimize(new IndexOptimizeResult(index.Name), CancellationToken.None);
        Indexes.WaitForIndexing(_store);

        Console.WriteLine($"[SETUP] merged '{index.Name}' into one segment in {optimizing.Elapsed:hh\\:mm\\:ss}");
    }

    public void AssertMatches(RavenSearchEngineMode engine, int matches)
    {
        using (var session = _store.OpenSession())
        {
            var actual = Where(session.Advanced.DocumentQuery<Coin>(CoinIndex.NameFor(engine)).NoCaching(), matches).Count();
            if (actual != matches)
                throw new InvalidOperationException($"The where clause meant to match {matches:N0} documents matched {actual:N0} on {engine}.");
        }
    }

    // the number of buckets: the facet's first page plus the terms beyond it
    public int Facet(RavenSearchEngineMode engine, int matches, bool withSum)
    {
        using (var session = _store.OpenSession())
        {
            var query = Where(session.Advanced.DocumentQuery<Coin>(CoinIndex.NameFor(engine)).NoCaching(), matches);

            var facets = withSum
                ? query.AggregateBy(f => f.ByField(x => x.MerchantId).WithOptions(FirstPage).SumOn(x => x.Amount)).Execute()
                : query.AggregateBy(f => f.ByField(x => x.MerchantId).WithOptions(FirstPage)).Execute();

            var result = facets[nameof(Coin.MerchantId)];
            return result.Values.Count + result.RemainingTermsCount;
        }
    }

    private IDocumentQuery<Coin> Where(IDocumentQuery<Coin> query, int matches)
    {
        if (matches == FewMatches)
        {
            return query
                .WhereIn(x => x.CustomerId, CustomerIds).AndAlso()
                .WhereGreaterThanOrEqual(x => x.CreatedAt, From).AndAlso()
                .WhereLessThan(x => x.CreatedAt, To).AndAlso()
                .WhereIn(x => x.EntityType, EntityTypes);
        }

        // every customer holds the same number of documents, so the number of customers sets the number of matches
        var customers = matches / (_documents / CustomerCount);
        return query.WhereIn(x => x.CustomerId, Enumerable.Range(0, customers).Select(i => $"customers/{i}"));
    }

    public override void Dispose()
    {
        _store?.Dispose();
        base.Dispose();
    }

    private class Coin
    {
        public string Id { get; set; }
        public string CustomerId { get; set; }
        // the facet field, one of MerchantCount distinct values
        public string MerchantId { get; set; }
        public string EntityType { get; set; }
        public DateTime CreatedAt { get; set; }
        public decimal Amount { get; set; }
    }

    // the same map, once per engine, so a single seeded database serves both
    private class CoinIndex : AbstractIndexCreationTask<Coin>
    {
        private readonly RavenSearchEngineMode _engine;

        public static string NameFor(RavenSearchEngineMode engine) => $"CoinIndex/{engine}";

        public override string IndexName => NameFor(_engine);

        public CoinIndex(RavenSearchEngineMode engine)
        {
            _engine = engine;

            Map = coins => from coin in coins
                           select new
                           {
                               coin.CustomerId,
                               coin.MerchantId,
                               coin.EntityType,
                               coin.CreatedAt,
                               coin.Amount
                           };

            Configuration[RavenConfiguration.GetKey(x => x.Indexing.StaticIndexingEngineType)] = engine.ToString();
        }
    }
}

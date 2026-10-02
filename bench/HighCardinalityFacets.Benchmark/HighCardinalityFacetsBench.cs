using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using BenchmarkDotNet.Attributes;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Server.Config;
using Tests.Infrastructure;
using Xunit.Abstractions;

namespace HighCardinalityFacets.Benchmark;

/// <summary>
/// A where clause matching a few hundred documents, faceted over a field with 250k distinct values, with and without an
/// aggregation, on both engines. A term facet used to cost a full pass over the facet field's terms on every query,
/// however few documents matched, so the facet dwarfed the query itself on high cardinality fields such as ids.
/// One database is seeded once for the whole run and indexed by both engines, each through its own index.
/// </summary>
public class HighCardinalityFacetsBench
{
    private Harness _harness;

    [Params(RavenSearchEngineMode.Corax, RavenSearchEngineMode.Lucene)]
    public RavenSearchEngineMode Engine { get; set; }

    [Params(1_000_000)]
    public int Documents { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _harness = Harness.Shared(Documents);
    }

    [Benchmark]
    public int CountByMerchant() => _harness.Facet(Engine, withSum: false);

    [Benchmark]
    public int SumAmountByMerchant() => _harness.Facet(Engine, withSum: true);
}

public class Harness : RavenTestBase
{
    private const int MerchantCount = 250_000;
    private const int ProgressEvery = 1_000_000;
    private static readonly TimeSpan IndexingTimeout = TimeSpan.FromHours(1);

    private static readonly DateTime Base = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime From = Base.AddDays(30);
    private static readonly DateTime To = Base.AddDays(90).AddSeconds(30);
    private static readonly string[] CustomerIds = Enumerable.Range(0, 5).Select(i => $"customers/{i}").ToArray();
    private static readonly string[] EntityTypes = { "Deposit" };

    // ConsoleTestOutputHelper marks the RavenTestBase instance as running outside of xUnit
    private static readonly ConsoleTestOutputHelper TestOutputHelper = new ConsoleTestOutputHelper();
    private static readonly object SharedLock = new object();
    private static Harness _shared;

    private IDocumentStore _store;

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
                    CustomerId = $"customers/{i % 200}",
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

        var coraxBuckets = Facet(RavenSearchEngineMode.Corax, withSum: true);
        var luceneBuckets = Facet(RavenSearchEngineMode.Lucene, withSum: true);
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

    public int Facet(RavenSearchEngineMode engine, bool withSum)
    {
        using (var session = _store.OpenSession())
        {
            var query = session.Advanced.DocumentQuery<Coin>(CoinIndex.NameFor(engine))
                .NoCaching()
                .WhereIn(x => x.CustomerId, CustomerIds).AndAlso()
                .WhereGreaterThanOrEqual(x => x.CreatedAt, From).AndAlso()
                .WhereLessThan(x => x.CreatedAt, To).AndAlso()
                .WhereIn(x => x.EntityType, EntityTypes);

            var facets = withSum
                ? query.AggregateBy(f => f.ByField(x => x.MerchantId).SumOn(x => x.Amount)).Execute()
                : query.AggregateBy(f => f.ByField(x => x.MerchantId)).Execute();

            return facets[nameof(Coin.MerchantId)].Values.Count;
        }
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

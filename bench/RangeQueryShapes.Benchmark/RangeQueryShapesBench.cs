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

namespace RangeQueryShapes.Benchmark;

/// <summary>
/// Compares the three ways a lower and an upper bound on one field reach the server: nested the way the .NET LINQ
/// provider emits it, flat, and as an explicit 'between'. The server folds any 'and' chain holding such a pair into a
/// single between query, so all three are expected to run the same; a gap between them means the fold stopped working.
/// One database is seeded once for the whole run and indexed by both engines, each through its own index.
/// </summary>
public class RangeQueryShapesBench
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

    [Benchmark(Baseline = true)]
    public int FlatPair() => _harness.Query(Engine, Harness.FlatWhere);

    [Benchmark]
    public int NestedPair() => _harness.Query(Engine, Harness.NestedWhere);

    [Benchmark]
    public int ExplicitBetween() => _harness.Query(Engine, Harness.BetweenWhere);
}

public class Harness : RavenTestBase
{
    // what the .NET LINQ provider emits for 'a.In(..) && x >= from && x < to && b.In(..)'
    public const string NestedWhere = "((CustomerId in ($p0) and CreatedAt >= $p1) and CreatedAt < $p2) and EntityType in ($p3)";
    public const string FlatWhere = "CustomerId in ($p0) and CreatedAt >= $p1 and CreatedAt < $p2 and EntityType in ($p3)";
    public const string BetweenWhere = "CustomerId in ($p0) and CreatedAt between $p1 and $p2 and EntityType in ($p3)";

    private static readonly DateTime Base = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime From = Base.AddDays(30);
    // 30 seconds past a minute boundary so no document sits exactly on the upper bound,
    // which keeps the inclusive 'between' and the half-open pair equivalent for this data
    private static readonly DateTime To = Base.AddDays(90).AddSeconds(30);
    private static readonly string[] CustomerIds = Enumerable.Range(0, 5).Select(i => $"customers/{i}").ToArray();
    private static readonly string[] EntityTypes = { "Deposit" };

    private const int ProgressEvery = 1_000_000;
    private static readonly TimeSpan IndexingTimeout = TimeSpan.FromHours(12);

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

        // persist to disk, a data set of this size does not fit the in-memory pager
        var options = new Options { RunInMemory = false };
        _store = GetDocumentStore(options);

        new CoinIndex(RavenSearchEngineMode.Corax).Execute(_store);
        new CoinIndex(RavenSearchEngineMode.Lucene).Execute(_store);

        Console.WriteLine($"[SETUP] seeding {documents:N0} documents into '{_store.Database}' at {DateTime.Now:HH:mm:ss}");

        // one document per minute, so CreatedAt has as many distinct terms as there are documents
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

        foreach (var engine in new[] { RavenSearchEngineMode.Corax, RavenSearchEngineMode.Lucene })
        {
            var expected = Query(engine, FlatWhere);
            if (expected == 0)
                throw new InvalidOperationException($"{engine}: the benchmark query matched nothing, the data or the window is wrong.");

            foreach (var where in new[] { NestedWhere, BetweenWhere })
            {
                var count = Query(engine, where);
                if (count != expected)
                    throw new InvalidOperationException($"{engine}: '{where}' returned {count} documents, expected {expected}.");
            }

            Console.WriteLine($"[SETUP] engine={engine} documents={documents:N0} matching={expected}");
        }

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

            Thread.Sleep(TimeSpan.FromSeconds(60));
        }
    }

    public int Query(RavenSearchEngineMode engine, string where)
    {
        using (var session = _store.OpenSession())
        {
            return session.Advanced
                .RawQuery<Coin>($"from index '{CoinIndex.NameFor(engine)}' where {where}")
                .AddParameter("p0", CustomerIds)
                .AddParameter("p1", From)
                .AddParameter("p2", To)
                .AddParameter("p3", EntityTypes)
                .NoCaching()
                .Take(int.MaxValue)
                .ToList()
                .Count;
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
                               coin.EntityType,
                               coin.CreatedAt,
                               coin.Amount
                           };

            Configuration[RavenConfiguration.GetKey(x => x.Indexing.StaticIndexingEngineType)] = engine.ToString();
        }
    }
}

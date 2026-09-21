using System;
using System.Linq;
using BenchmarkDotNet.Attributes;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit.Abstractions;

namespace RangeQueryShapes.Benchmark;

/// <summary>
/// Compares the three ways a lower and an upper bound on one field reach the server: nested the way the .NET LINQ
/// provider emits it, flat, and as an explicit 'between'. The server folds any 'and' chain holding such a pair into a
/// single between query, so all three are expected to run the same; a gap between them means the fold stopped working.
/// </summary>
public class RangeQueryShapesBench
{
    // ConsoleTestOutputHelper marks the RavenTestBase instance as running outside of xUnit
    private static readonly ConsoleTestOutputHelper TestOutputHelper = new ConsoleTestOutputHelper();

    private Harness _harness;

    [Params(RavenSearchEngineMode.Corax, RavenSearchEngineMode.Lucene)]
    public RavenSearchEngineMode Engine { get; set; }

    [Params(1_000_000)]
    public int Documents { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _harness = new Harness(TestOutputHelper);
        _harness.Initialize(Engine, Documents);
    }

    [Benchmark(Baseline = true)]
    public int FlatPair() => _harness.Query(Harness.FlatWhere);

    [Benchmark]
    public int NestedPair() => _harness.Query(Harness.NestedWhere);

    [Benchmark]
    public int ExplicitBetween() => _harness.Query(Harness.BetweenWhere);

    [GlobalCleanup]
    public void Cleanup()
    {
        _harness?.Dispose();
        _harness = null;
    }
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

    private IDocumentStore _store;
    private int _expectedCount;

    public Harness(ITestOutputHelper output) : base(output)
    {
    }

    public void Initialize(RavenSearchEngineMode engine, int documents)
    {
        _store = GetDocumentStore(Options.ForSearchEngine(engine));

        new CoinIndex().Execute(_store);

        // one document per minute, so CreatedAt has as many distinct terms as there are documents
        var entityTypes = new[] { "Deposit", "Withdrawal", "Transfer" };
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
            }
        }

        Indexes.WaitForIndexing(_store, timeout: TimeSpan.FromMinutes(10));

        _expectedCount = Query(FlatWhere);
        if (_expectedCount == 0)
            throw new InvalidOperationException("The benchmark query matched nothing, the data or the window is wrong.");

        foreach (var where in new[] { NestedWhere, BetweenWhere })
        {
            var count = Query(where);
            if (count != _expectedCount)
                throw new InvalidOperationException($"'{where}' returned {count} documents, expected {_expectedCount}.");
        }

        Console.WriteLine($"[SETUP] engine={engine} documents={documents} matching={_expectedCount}");
    }

    public int Query(string where)
    {
        using (var session = _store.OpenSession())
        {
            return session.Advanced
                .RawQuery<Coin>($"from index 'CoinIndex' where {where}")
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

    private class CoinIndex : AbstractIndexCreationTask<Coin>
    {
        public CoinIndex()
        {
            Map = coins => from coin in coins
                           select new
                           {
                               coin.CustomerId,
                               coin.EntityType,
                               coin.CreatedAt,
                               coin.Amount
                           };
        }
    }
}

using System;
using System.Collections.Concurrent;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Engines;

namespace Micro.Benchmark.Benchmarks
{
    /// <summary>
    /// Isolates the plan-template lookup on the Corax query hot path: a string-keyed ConcurrentDictionary probe
    /// vs the QueryMetadata memo (long Id compare plus WeakReference.TryGetTarget). A plain object stands in for
    /// the real PlanTemplate since the payload type is irrelevant to lookup cost.
    /// </summary>
    [DisassemblyDiagnoser]
    [SimpleJob(RunStrategy.Throughput, RuntimeMoniker.Net10_0, warmupCount: 5, iterationCount: 10)]
    public class PlanTemplateLookupBenchmark
    {
        // A representative auto-index RQL string — the key the dictionary must hash and compare on every probe.
        private const string QueryText =
            "from index 'Auto/Movies/ByGenresAndReleaseDate' where Genres = $genre and ReleaseDate >= $date order by ReleaseDate as string desc";

        private ConcurrentDictionary<string, object> _current;
        private ConcurrentDictionary<string, object> _previous;
        private readonly object _template = new();

        // Memo state: the value-typed identity token stamped on the live cache, plus the weakly-held template.
        private long _liveCacheId;
        private long _memoCacheId;
        private WeakReference<object> _memoTemplate;

        [Params(1, 8, 64)]
        public int DistinctQueries;

        [GlobalSetup]
        public void Setup()
        {
            _current = new ConcurrentDictionary<string, object>(StringComparer.Ordinal);
            _previous = new ConcurrentDictionary<string, object>(StringComparer.Ordinal);

            // Populate with decoy entries so the dictionary isn't a degenerate single-bucket probe.
            for (int i = 0; i < DistinctQueries - 1; i++)
                _current[QueryText + " /*" + i + "*/"] = new object();
            _current[QueryText] = _template;

            _liveCacheId = 42;
            _memoCacheId = 42; // memo was stamped against the live cache => hit
            _memoTemplate = new WeakReference<object>(_template);
        }

        // Dictionary path: TryGetValue on current, falling back to previous on a miss.
        [Benchmark(Baseline = true)]
        public object DictionaryLookup()
        {
            if (_current.TryGetValue(QueryText, out var per) == false)
                _previous.TryGetValue(QueryText, out per);
            return per;
        }

        // Memo fast path: Id compare (rejects a stale index instance) then a weak deref.
        [Benchmark]
        public object MemoLookup()
        {
            if (_memoCacheId == _liveCacheId && _memoTemplate.TryGetTarget(out var template))
                return template;

            // Slow-path fallback (not exercised on a hit) — kept so the JIT can't elide the branch entirely.
            if (_current.TryGetValue(QueryText, out var per) == false)
                _previous.TryGetValue(QueryText, out per);
            return per;
        }
    }
}

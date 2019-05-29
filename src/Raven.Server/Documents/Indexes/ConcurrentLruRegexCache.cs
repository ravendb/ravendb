using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;

namespace Raven.Server.Documents.Indexes
{
    public class ConcurrentLruRegexCache
    {
        public ConcurrentLruRegexCache(int capacity)
        {
            _capacity = capacity;
            _halfCapacity = _capacity / 2;
            Debug.Assert(_halfCapacity > 0);
        }
        private readonly ConcurrentDictionary<string, ConcurrentLruRegexCacheNode> _regexCache = new ConcurrentDictionary<string, ConcurrentLruRegexCacheNode>();
        private readonly int _capacity;
        private long _count;
        private bool _neverCompile;
        private DateTime _lastClearTime = DateTime.MinValue;
        private readonly int _halfCapacity;

        public Regex Get(string pattern)
        {
            if (!_regexCache.TryGetValue(pattern, out var result))
                return GetUnlikely(pattern); // create it

            // we don't care about this until the cache is over half full,
            // so this should be called if user is using a reasonable number
            // of regex queries.
            if (_count > _halfCapacity)
                UpdateTimestamp(result);
            return result.RegexLazy.Value; 
        }

        private static void UpdateTimestamp(ConcurrentLruRegexCacheNode result)
        {
            var timestamp = Stopwatch.GetTimestamp();

            var resultTimestamp = result.Timestamp;
            // here we want to avoid updating this using Interlocked on a highly
            // used query, so we limit it to once per 10,000 ticks, which should
            // still be good enough if we need to start evicting
            if (timestamp - resultTimestamp > 10_000)//if our snapshot is over 10,000 ticks old
            {
                //If we fail this interlocked operation, it means that some other thread already updated the timestamp
                Interlocked.CompareExchange(ref result.Timestamp, timestamp, resultTimestamp);
            }
        }

        private Regex GetUnlikely(string pattern)
        {
            var result = new ConcurrentLruRegexCacheNode(
                pattern, _neverCompile ? RegexOptions.None : RegexOptions.Compiled)
            {
                Timestamp = Stopwatch.GetTimestamp()
            };

            var res = _regexCache.GetOrAdd(pattern, result);

            if (res != result) // someone else created it
                return res.RegexLazy.Value;

            //We have reached the capacity and we will now clear 25% of the cache
            var currentCount = Interlocked.Increment(ref _count);
            if (currentCount >= _capacity)
                ClearOldItems();
            return res.RegexLazy.Value;
        }

        private void ClearOldItems()
        {
            bool lockTaken = false;
            try
            {
                Monitor.TryEnter(this, 0, ref lockTaken);
                if (lockTaken == false)
                    return;

                if (Interlocked.Read(ref _count) < _capacity)
                    return;

                if ((DateTime.UtcNow - _lastClearTime).TotalMinutes < 1)
                {
                    //This is the second time we reached 100% capacity within a minute giving up on compiling
                    _neverCompile = true; 
                }
                //Dropping 25% of the cached items
                int countRemoved = 0;
                foreach (var kv in _regexCache
                    .OrderBy(kv => kv.Value.Timestamp)
                    .Take(_capacity / 4))
                {
                    
                    if (_regexCache.TryRemove(kv.Key, out _))
                        countRemoved++;
                }
                Interlocked.Add(ref _count, -countRemoved);

                _lastClearTime = DateTime.UtcNow;
            }
            finally
            {
                if (lockTaken)
                    Monitor.Exit(this);
            }
        }
    }

    internal class ConcurrentLruRegexCacheNode
    {
        public long Timestamp;
        public Lazy<Regex> RegexLazy { get; }

        public ConcurrentLruRegexCacheNode(string pattern, RegexOptions options = RegexOptions.None)
        {
            var flags = RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | options;
            RegexLazy = new Lazy<Regex>(()=>new Regex(pattern, flags, 
                // we use 50 ms as the max timeout because this is going to be evaluated
                // on _each_ term in the results, potentially millions, so we specify a very
                // short value to avoid very long queries
                TimeSpan.FromMilliseconds(50)));
        }
    }
}

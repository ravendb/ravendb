using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using Sparrow.Json.Parsing;

namespace Raven.Server.Utils.Metrics
{
    // Sliding-window histogram over a fixed quantum (100ms).
    // - O(1) write path with minimal contention (single atomic per field)
    // - Time-bounded reads that ignore stale buckets strictly outside the requested window
    // - Stable moving-average semantics (boxcar) with bounded boundary bias (<= one quantum)
    public sealed class MeterMetric
    {
        private const double BucketDurationSeconds = 0.1;

        private static readonly long BucketDurationNanoseconds = (long)(BucketDurationSeconds * Clock.NanosecondsInSecond);
        private static readonly TimeSpan MaxWindow = TimeSpan.FromMinutes(15);
        private static readonly long MaxWindowNanoseconds = (long)(MaxWindow.TotalSeconds * Clock.NanosecondsInSecond);
        private static readonly int BucketCount = (int)(MaxWindowNanoseconds / BucketDurationNanoseconds) + 2;

        private struct Bucket
        {
            public long Count;
            public long DurationSum;
            public long Start;
        }

        // Ring-buffer of aggregated quanta. Each bucket carries its quantum start timestamp so readers can
        // distinguish fresh from wrapped/stale data without scanning or zeroing the whole array.
        private readonly Bucket[] _buckets = new Bucket[BucketCount];

        private long _count;
        private readonly long _startTime;
        private readonly Func<long> _now;

        private static long DefaultNow() => Clock.Nanoseconds;

        public MeterMetric(Func<long> nowProvider = null)
        {
            _now = nowProvider ?? DefaultNow;
            _startTime = _now();
        }

        public double OneSecondRate => GetRate(TimeSpan.FromSeconds(1));
        public double FiveSecondRate => GetRate(TimeSpan.FromSeconds(5));
        public double OneMinuteRate => GetRate(TimeSpan.FromMinutes(1));
        public double FiveMinuteRate => GetRate(TimeSpan.FromMinutes(5));
        public double FifteenMinuteRate => GetRate(TimeSpan.FromMinutes(15));

        internal double GetRate(int seconds)
        {
            return seconds <= 0 ? 0d : GetRate(TimeSpan.FromSeconds(seconds));
        }

        internal int GetIntRate(int seconds)
        {
            return (int)Math.Ceiling(GetRate(seconds));
        }

        public double MeanRate => GetMeanRate(_now() - _startTime);

        public long Count => Volatile.Read(ref _count);

        public void Tick()
        {
            // Kept for compatibility with existing schedulers; all updates happen in Mark().
        }

        public void Mark(long value, long duration = 0)
        {
            if (value == 0 && duration == 0)
                return;

            var now = _now();

            // Record completions into the current quantum and keep a monotonic total (for MeanRate/diagnostics).
            AddToBucket(now, value, duration);

            if (value != 0)
                Interlocked.Add(ref _count, value);
        }

        public void MarkSingleThreaded(long value, long duration = 0)
        {
            // Callers with single-threaded guarantees take this path today; forward to the main implementation
            // to avoid diverging semantics and keep JIT optimisations focused in one place.
            Mark(value, duration);
        }

        public double GetMeanRate(double elapsed)
        {
            var total = Volatile.Read(ref _count);
            if (elapsed <= 0)
                return 0.0;

            return total / elapsed * Clock.NanosecondsInSecond;
        }

        public void Mark()
        {
            Mark(1L);
        }

        public DynamicJsonValue CreateMeterData(bool allResults = false, bool filterEmpty = true)
        {
            var result = new DynamicJsonValue
            {
                ["Current"] = Math.Round(OneSecondRate, 1),
                ["Count"] = Count,
                ["MeanRate"] = Math.Round(MeanRate, 1),
                ["OneMinuteRate"] = Math.Round(OneMinuteRate, 1),
                ["FiveMinuteRate"] = Math.Round(FiveMinuteRate, 1),
                ["FifteenMinuteRate"] = Math.Round(FifteenMinuteRate, 1)
            };

            if (allResults == false)
                return result;

            var nowNs = _now();
            var nowUtc = DateTime.UtcNow;
            var bucketSeconds = BucketDurationNanoseconds / (double)Clock.NanosecondsInSecond;

            var buckets = new List<(long Start, double Rate)>();
            for (int i = 0; i < BucketCount; i++)
            {
                ref var bucket = ref _buckets[i];
                var start = Volatile.Read(ref bucket.Start);
                if (start == 0)
                    continue;

                if (nowNs - start > MaxWindowNanoseconds)
                    continue;

                var count = Volatile.Read(ref bucket.Count);
                if (filterEmpty && count == 0)
                    continue;

                buckets.Add((start, count / bucketSeconds));
            }

            buckets.Sort((a, b) => a.Start.CompareTo(b.Start));

            // Emit raw 100ms samples so diagnostics can plot the exact workload shape (no smoothing).
            var raw = new DynamicJsonValue();
            foreach (var (start, rate) in buckets)
            {
                var ageSeconds = (nowNs - start) / (double)Clock.NanosecondsInSecond;
                var timestamp = nowUtc - TimeSpan.FromSeconds(ageSeconds);
                raw[timestamp.ToString("O", CultureInfo.InvariantCulture)] = Math.Round(rate, 1);
            }

            result["Raw"] = raw;
            return result;
        }

        // A small CAS loop guards bucket advancement to avoid the "clear-after-publish" race.
        // Protocol:
        // - Writers claim a bucket by CAS-ing Start to a sentinel value (ClaimedStart).
        // - Under the claim, we zero Count/DurationSum, then publish the real Start via a release write.
        // - Readers acquire Start and skip buckets with Start <= 0 (never used or claimed/in-progress).
        // This ensures no thread observes half-initialized state. The loop runs only at quantum boundaries.
        private void AddToBucket(long timestamp, long value, long duration)
        {
            var quantum = timestamp / BucketDurationNanoseconds;
            var bucketStart = quantum * BucketDurationNanoseconds;
            var index = (int)(quantum % BucketCount);

            ref var bucket = ref _buckets[index];

            const long ClaimedStart = -1; // sentinel: bucket claimed for reinitialization

            while (true)
            {
                var start = Volatile.Read(ref bucket.Start);
                if (start == bucketStart)
                    break; // already advanced for this quantum

                if (start > 0 && bucketStart < start)
                {
                    // Another thread advanced further; follow it to avoid going backward.
                    bucketStart = start;
                    break;
                }

                if (start == ClaimedStart)
                    continue; // someone else is initializing; wait until they publish

                // Try to claim for reinitialization
                if (Interlocked.CompareExchange(ref bucket.Start, ClaimedStart, start) != start)
                    continue; // Lost the race; retry

                // Own the bucket under the claim: zero fields, then publish with release semantics
                Volatile.Write(ref bucket.Count, 0);
                Volatile.Write(ref bucket.DurationSum, 0);
                Volatile.Write(ref bucket.Start, bucketStart);
                break;
            }

            if (value != 0)
                Interlocked.Add(ref bucket.Count, value);
            if (duration != 0)
                Interlocked.Add(ref bucket.DurationSum, duration);
        }

        private double GetRate(TimeSpan window)
        {
            if (window <= TimeSpan.Zero)
                return 0.0;

            var windowNs = (long)(window.TotalSeconds * Clock.NanosecondsInSecond);
            if (windowNs <= 0)
                return 0.0;

            var total = Accumulate(windowNs, out _, out _);

            // Convert the aggregated completions into requests-per-second for the requested window. Buckets that
            // only partially overlap the range naturally contribute their full count, which matches the behaviour
            // expected from the mean rate (consistent with a boxcar moving average).
            return total / window.TotalSeconds;
        }

        public double GetAverageDuration(TimeSpan window)
        {
            if (window <= TimeSpan.Zero)
                return 0.0;

            var windowNs = (long)(window.TotalSeconds * Clock.NanosecondsInSecond);
            if (windowNs <= 0)
                return 0.0;

            var total = Accumulate(windowNs, out var totalDuration, out _);
            if (total <= 0)
                return 0.0;

            return totalDuration / (double)total;
        }

        public double GetAverageDuration()
        {
            return GetAverageDuration(TimeSpan.FromMinutes(1));
        }

        private long Accumulate(long windowNs, out long totalDuration, out long bucketsExamined)
        {
            var now = _now();
            var lowerBound = now - windowNs;

            var total = 0L;
            totalDuration = 0L;
            bucketsExamined = 0L;

            for (int i = 0; i < BucketCount; i++)
            {
                ref var bucket = ref _buckets[i];
                // Skip buckets that were never initialised or fully pre-date the window. Only buckets whose
                // quantum overlaps the requested range contribute to the final aggregates.
                var start = Volatile.Read(ref bucket.Start);
                if (start <= 0 || start + BucketDurationNanoseconds <= lowerBound)
                    continue;

                total += Volatile.Read(ref bucket.Count);
                totalDuration += Volatile.Read(ref bucket.DurationSum);
                bucketsExamined++;
            }

            return total;
        }
    }
}

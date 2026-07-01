using System;
using System.Collections.Generic;
using System.Linq;
using Tests.Infrastructure;
using Voron.Data.Lookups;
using Xunit;

namespace FastTests.Voron
{
    public class LookupRangeCount : StorageTest
    {
        public LookupRangeCount(ITestOutputHelper output) : base(output)
        {
        }

        private static long BruteForce(List<long> keys, long low, long high)
        {
            long count = 0;
            foreach (var k in keys)
            {
                if (k >= low && k <= high)
                    count++;
            }
            return count;
        }

        private void AssertMatchesOracle(List<long> keys, long low, long high)
        {
            using var rtx = Env.ReadTransaction();
            var lookup = rtx.LookupFor<Int64LookupKey>("test");
            long expected = BruteForce(keys, low, high);
            long actual = lookup.GetNumberOfEntriesInRange(low, high);
            Assert.Equal(expected, actual);
        }

        // The sampling estimator is compared against the exact oracle. We allow a relative slack (the structural
        // sampling assumes uniform fan-out / leaf fill) plus a small absolute floor so tiny counts -- where the
        // estimator falls back to exact same-leaf counting -- don't trip on rounding.
        private void AssertEstimateClose(List<long> keys, long low, long high, double relTol, long absFloor)
        {
            using var rtx = Env.ReadTransaction();
            var lookup = rtx.LookupFor<Int64LookupKey>("test");
            long expected = BruteForce(keys, low, high);
            long actual = lookup.GetNumberOfEntriesInRangeEstimate(low, lowToStart: false, high: high, highToEnd: false);

            long allowed = Math.Max(absFloor, (long)(expected * relTol));
            long diff = Math.Abs(actual - expected);
            Assert.True(diff <= allowed,
                $"estimate {actual} vs exact {expected} for [{low}, {high}]: |diff|={diff} > allowed={allowed}");
        }

        private List<long> Seed(IEnumerable<long> values)
        {
            var keys = values.ToList();
            using (var wtx = Env.WriteTransaction())
            {
                var lookup = wtx.LookupFor<Int64LookupKey>("test");
                foreach (var k in keys)
                    lookup.Add(k, k);
                wtx.Commit();
            }
            keys.Sort();
            return keys;
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void DenseUniform_MatchesOracle()
        {
            // enough keys to force a multi-level tree (root + branches + leaves)
            var keys = Seed(Enumerable.Range(0, 50_000).Select(i => (long)i));

            AssertMatchesOracle(keys, 100, 200);          // small interior range
            AssertMatchesOracle(keys, 0, 49_999);         // full range
            AssertMatchesOracle(keys, -100, 100);         // low below first
            AssertMatchesOracle(keys, 49_900, 60_000);    // high beyond last
            AssertMatchesOracle(keys, 25_000, 25_000);    // single value
            AssertMatchesOracle(keys, 30_000, 10_000);    // inverted -> 0
            AssertMatchesOracle(keys, 12_345, 38_765);    // large interior range
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void WithGaps_MatchesOracle()
        {
            // keys at multiples of 7 only -> bounds frequently land between stored keys
            var keys = Seed(Enumerable.Range(0, 20_000).Select(i => (long)i * 7));

            AssertMatchesOracle(keys, 0, 7 * 20_000);     // full
            AssertMatchesOracle(keys, 50, 100);           // both bounds land in gaps
            AssertMatchesOracle(keys, 7, 7);              // exact stored key
            AssertMatchesOracle(keys, 8, 13);             // gap-only window -> 0
            AssertMatchesOracle(keys, 69_993, 70_007);    // straddles a stored key
            AssertMatchesOracle(keys, -1, 6);             // before-first window
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void Sparse_MatchesOracle()
        {
            var keys = Seed(new long[] { 5, 17, 42, 99, 1000 });

            AssertMatchesOracle(keys, long.MinValue, long.MaxValue);
            AssertMatchesOracle(keys, 5, 99);
            AssertMatchesOracle(keys, 6, 41);   // between 5 and 42 -> only 17
            AssertMatchesOracle(keys, 1001, 5000); // after last -> 0
            AssertMatchesOracle(keys, 100, 999);   // empty interior -> 0
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void EmptyTree_ReturnsZero()
        {
            Seed(Enumerable.Empty<long>());
            AssertMatchesOracle(new List<long>(), long.MinValue, long.MaxValue);
            AssertMatchesOracle(new List<long>(), 0, 100);
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void Estimate_DenseUniform_CloseToOracle()
        {
            // large, maximally uniform tree -> sampling should be very accurate on the wide ranges
            var keys = Seed(Enumerable.Range(0, 200_000).Select(i => (long)i));

            AssertEstimateClose(keys, 0, 199_999, 0.10, 200);        // full range
            AssertEstimateClose(keys, 50_000, 150_000, 0.10, 200);   // wide interior band
            AssertEstimateClose(keys, 12_345, 187_654, 0.10, 200);   // wide off-center band
            AssertEstimateClose(keys, 0, 100_000, 0.10, 200);        // half, anchored at the bottom
            AssertEstimateClose(keys, 100_000, 199_999, 0.10, 200);  // half, anchored at the top
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void Estimate_SmallRanges_AreExact()
        {
            // small ranges resolve within a single (or adjacent) leaf, so the estimator returns the exact count
            var keys = Seed(Enumerable.Range(0, 200_000).Select(i => (long)i));

            AssertEstimateClose(keys, 25_000, 25_000, 0.0, 0);   // single value
            AssertEstimateClose(keys, 100, 200, 0.0, 0);         // tiny interior window
            AssertEstimateClose(keys, -100, 100, 0.0, 0);        // straddles the low edge
            AssertEstimateClose(keys, 199_900, 250_000, 0.0, 0); // straddles the high edge
            AssertEstimateClose(keys, 30_000, 10_000, 0.0, 0);   // inverted -> 0
        }

        // An open high bound descends the rightmost leaf instead of seeking a concrete key, so the estimate must
        // cover everything from low to the end of the tree regardless of the (ignored) high argument.
        private void AssertOpenHighEstimateClose(List<long> keys, long low, double relTol, long absFloor)
        {
            using var rtx = Env.ReadTransaction();
            var lookup = rtx.LookupFor<Int64LookupKey>("test");
            long expected = BruteForce(keys, low, long.MaxValue);
            // pass an arbitrary (here: deliberately wrong, far-below) high to prove it is ignored when highToEnd
            long actual = lookup.GetNumberOfEntriesInRangeEstimate(low, lowToStart: false, high: low, highToEnd: true);

            long allowed = Math.Max(absFloor, (long)(expected * relTol));
            long diff = Math.Abs(actual - expected);
            Assert.True(diff <= allowed,
                $"open-high estimate {actual} vs exact {expected} for [{low}, +inf): |diff|={diff} > allowed={allowed}");
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void Estimate_OpenHighBound_CountsToEnd()
        {
            var keys = Seed(Enumerable.Range(0, 200_000).Select(i => (long)i));

            AssertOpenHighEstimateClose(keys, 0, 0.10, 200);         // entire tree
            AssertOpenHighEstimateClose(keys, 100_000, 0.10, 200);   // upper half
            AssertOpenHighEstimateClose(keys, 187_654, 0.10, 200);   // small tail near the top
            AssertOpenHighEstimateClose(keys, 199_990, 0.0, 16);     // tiny tail resolves within the last leaf
            AssertOpenHighEstimateClose(keys, 250_000, 0.0, 0);      // low past the max -> 0
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void Estimate_WithGaps_CloseToOracle()
        {
            // bounds frequently land between stored keys; term-slot sampling is unaffected by the gaps
            var keys = Seed(Enumerable.Range(0, 100_000).Select(i => (long)i * 7));

            AssertEstimateClose(keys, 0, 7 * 100_000, 0.10, 200);  // full
            AssertEstimateClose(keys, 7 * 10_000, 7 * 90_000, 0.10, 200);
            AssertEstimateClose(keys, 50, 100, 0.0, 0);            // gap-only window resolves exactly
        }
    }
}

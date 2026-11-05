using Tests.Infrastructure;
using Voron.Impl;
using Xunit;
using Constants = Voron.Global.Constants;

namespace FastTests.Voron.Paging
{
    // Only arithmetic on the base address: the address is never dereferenced.
    public unsafe class PagerStateShouldPrefetchSegmentTests
    {
        private const long SegmentSize = 4 * 1024 * 1024;
        private const long PagesInSegment = SegmentSize / Constants.Storage.PageSize;
        private static readonly byte* Base = (byte*)0x10000000;

        private static PagerState Create(long sizeInBytes, long resetThreshold = 1_000_000 * SegmentSize)
        {
            var allocation = new PagerState.AllocationInfo { BaseAddress = Base, Size = sizeInBytes, MappedFile = null };
            return new PagerState(null, SegmentSize, resetThreshold, allocation);
        }

        private static bool Ask(PagerState state, long page, out long address, out long bytes)
        {
            var result = state.ShouldPrefetchSegment(page, out var virtualAddress, out bytes);
            address = (long)virtualAddress;
            return result;
        }

        private static long FirstPageOf(long segment) => segment * PagesInSegment;

        [RavenFact(RavenTestCategory.Voron)]
        public void TheFirstSegmentIsPrefetchedAtTheSeventhAccess()
        {
            var state = Create(10 * SegmentSize);

            for (var i = 1; i <= 6; i++)
                Assert.False(Ask(state, i, out _, out _), "Access " + i);

            Assert.True(Ask(state, 0, out var address, out var bytes));
            Assert.Equal((long)Base, address);
            Assert.Equal(SegmentSize, bytes);
            Assert.False(Ask(state, 1, out _, out _)); // and only once
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void AnIsolatedSegmentIsPrefetchedAtTheSeventhAccess()
        {
            var state = Create(10 * SegmentSize);
            var page = FirstPageOf(5);

            for (var i = 1; i <= 6; i++)
            {
                Assert.False(Ask(state, page + i, out var address, out var bytes), "Access " + i);
                Assert.Equal(0, address);
                Assert.Equal(0, bytes);
            }

            Assert.True(Ask(state, page, out var virtualAddress, out var size));
            Assert.Equal((long)Base + 5 * SegmentSize, virtualAddress);
            Assert.Equal(SegmentSize, size);
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void ASegmentIsNotRequestedTwice()
        {
            var state = Create(10 * SegmentSize);
            var page = FirstPageOf(5);

            var requests = 0;
            for (var i = 0; i < 1000; i++)
            {
                if (Ask(state, page + i % PagesInSegment, out _, out _))
                    requests++;
            }

            Assert.Equal(1, requests);
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void AnyPageOfTheSegmentCountsForTheSegment()
        {
            var state = Create(10 * SegmentSize);

            // the accesses below the threshold do not request, the next one does
            Assert.False(Ask(state, FirstPageOf(4), out _, out _));
            Assert.False(Ask(state, FirstPageOf(5) - 1, out _, out _)); // last page of segment 4
            Assert.False(Ask(state, FirstPageOf(4) + 100, out _, out _));
            Assert.False(Ask(state, FirstPageOf(4) + 200, out _, out _));
            Assert.False(Ask(state, FirstPageOf(4) + 300, out _, out _));
            Assert.False(Ask(state, FirstPageOf(4) + 400, out _, out _));
            Assert.True(Ask(state, FirstPageOf(5) - 1, out var address, out _));
            Assert.Equal((long)Base + 4 * SegmentSize, address);
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void TheSegmentAfterAPrefetchedOneIsPrefetchedAtTheFirstAccess()
        {
            var state = Create(10 * SegmentSize);
            PrefetchByCounting(state, 3);

            Assert.True(Ask(state, FirstPageOf(4), out var address, out var bytes));
            Assert.Equal((long)Base + 4 * SegmentSize, address);
            Assert.Equal(SegmentSize, bytes);
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void TheSegmentBeforeAPrefetchedOneIsPrefetchedAtTheFirstAccess()
        {
            var state = Create(10 * SegmentSize);
            PrefetchByCounting(state, 6);

            Assert.True(Ask(state, FirstPageOf(5), out var address, out _));
            Assert.Equal((long)Base + 5 * SegmentSize, address);
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void ASegmentFarFromThePrefetchedOnesIsNot()
        {
            var state = Create(10 * SegmentSize);
            PrefetchByCounting(state, 3);

            Assert.False(Ask(state, FirstPageOf(6), out _, out _));
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void ASequentialScanRequestsEverySegmentOnce()
        {
            const int segments = 20;
            var state = Create(segments * SegmentSize);
            var requested = new System.Collections.Generic.List<long>();

            for (long page = 0; page < segments * PagesInSegment; page++)
            {
                if (Ask(state, page, out var address, out var bytes))
                {
                    Assert.Equal(SegmentSize, bytes);
                    requested.Add((address - (long)Base) / SegmentSize);
                }
            }

            Assert.Equal(segments, requested.Count);
            for (var i = 0; i < segments; i++)
                Assert.Equal(i, requested[i]); // in order: each is requested at the first page of its segment
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void TheLastPartialSegmentIsPrefetched()
        {
            // two segments and a quarter
            var size = 2 * SegmentSize + SegmentSize / 4;
            var state = Create(size);
            var lastPage = size / Constants.Storage.PageSize - 1;

            PrefetchByCounting(state, 1);

            Assert.True(Ask(state, FirstPageOf(2), out var address, out _)); // next to a prefetched one
            Assert.Equal((long)Base + 2 * SegmentSize, address);
            Assert.False(Ask(state, lastPage, out _, out _)); // already done
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void TheLastPartialSegmentIsPrefetchedAfterSevenAccessesWhenAlone()
        {
            var size = 10 * SegmentSize + SegmentSize / 4;
            var state = Create(size);
            var lastPage = size / Constants.Storage.PageSize - 1;

            for (var i = 0; i < 6; i++)
                Assert.False(Ask(state, lastPage - i, out _, out _));

            Assert.True(Ask(state, lastPage, out var address, out _));
            Assert.Equal((long)Base + 10 * SegmentSize, address);
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void PagesBeyondTheEndOfTheFileAreNeverPrefetched()
        {
            var state = Create(10 * SegmentSize);

            foreach (var page in new[] { FirstPageOf(40), FirstPageOf(1000), long.MaxValue / Constants.Storage.PageSize / 2 })
            {
                for (var i = 0; i < 20; i++)
                    Assert.False(Ask(state, page, out var address, out var bytes));
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void NegativePagesAreNeverPrefetched()
        {
            var state = Create(10 * SegmentSize);

            for (var i = 0; i < 20; i++)
                Assert.False(Ask(state, -5 * PagesInSegment, out _, out _));
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void AnEmptyStateDoesNotThrow()
        {
            var state = new PagerState(null, SegmentSize, SegmentSize * 100);

            for (var i = 0; i < 20; i++)
                Ask(state, i * PagesInSegment, out _, out _);
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void ResettingTheTableMakesTheSegmentsEligibleAgain()
        {
            // resetting is done once more than the threshold of segments were requested
            var state = Create(10 * SegmentSize, resetThreshold: 2 * SegmentSize);
            PrefetchByCounting(state, 0);
            Assert.True(Ask(state, FirstPageOf(1), out _, out _));
            Assert.True(Ask(state, FirstPageOf(2), out _, out _));
            Assert.False(Ask(state, FirstPageOf(2), out _, out _));

            state.CheckResetPrefetchTable();

            PrefetchByCounting(state, 0); // asked for again
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void TheTableIsNotResetBeforeTheThreshold()
        {
            var state = Create(10 * SegmentSize, resetThreshold: 100 * SegmentSize);
            PrefetchByCounting(state, 0);

            state.CheckResetPrefetchTable();

            Assert.False(Ask(state, FirstPageOf(0), out _, out _));
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void TheStateIsCopiedToTheBiggerStateOfAGrownFile()
        {
            var old = Create(4 * SegmentSize);
            PrefetchByCounting(old, 0);
            Assert.True(Ask(old, FirstPageOf(1), out _, out _));

            var grown = Create(12 * SegmentSize);
            grown.CopyPrefetchState(old);

            Assert.False(Ask(grown, FirstPageOf(0), out _, out _)); // not requested again
            Assert.False(Ask(grown, FirstPageOf(1), out _, out _));
            Assert.True(Ask(grown, FirstPageOf(2), out var address, out _)); // next to a prefetched one
            Assert.Equal((long)Base + 2 * SegmentSize, address);
            // the new part of the file is prefetched when reached
            Assert.False(Ask(grown, FirstPageOf(9), out _, out _));
            for (var i = 0; i < 6; i++)
                Ask(grown, FirstPageOf(9), out _, out _);
            Assert.False(Ask(grown, FirstPageOf(9), out _, out _));
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void ASmallSegmentSizeIsRoundedToAPowerOfTwo()
        {
            var allocation = new PagerState.AllocationInfo { BaseAddress = Base, Size = 64 * 1024, MappedFile = null };
            var state = new PagerState(null, 64 * 1024, 1_000_000L * 64 * 1024, allocation);

            for (var i = 0; i < 6; i++)
                Assert.False(Ask(state, 0, out _, out _));

            Assert.True(Ask(state, 0, out var address, out var bytes));
            Assert.Equal((long)Base, address);
            Assert.Equal(64 * 1024, bytes);
        }

        // accesses the segment until it is requested
        private static void PrefetchByCounting(PagerState state, long segment)
        {
            for (var i = 0; i < 7; i++)
            {
                if (Ask(state, FirstPageOf(segment), out _, out _))
                    return;
            }

            Assert.Fail("Segment " + segment + " was not prefetched");
        }
    }
}

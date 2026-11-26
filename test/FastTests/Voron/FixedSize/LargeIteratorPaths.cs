using System;
using System.Diagnostics;
using Tests.Infrastructure;
using Voron;
using Voron.Data.Fixed;
using Voron.Impl;
using Xunit;

namespace FastTests.Voron.FixedSize
{
    public class LargeIteratorPaths(ITestOutputHelper output) : StorageTest(output)
    {
        // a leaf holds 8 entries with values of 1,000 bytes, so 6,000 entries make a tree of three levels
        private const int Entries = 6_000;
        private const ushort ValueSize = 1_000;

        [RavenFact(RavenTestCategory.Voron)]
        public void CountingTheEntriesAfterAKeyDoesNotMoveAnOpenIterator()
        {
            using var tx = Env.WriteTransaction();
            var fst = CreateTree(tx);

            using var it = fst.Iterate();
            Assert.True(it.Seek(0));
            for (long expected = 0; expected < Entries; expected++)
            {
                Assert.Equal(expected, it.CurrentKey);
                Assert.Equal(Entries - expected - 1, fst.GetNumberOfEntriesAfter(expected, out _, Stopwatch.StartNew(), EstimationAccuracy.Exact));
                Assert.Equal(expected < Entries - 1, it.MoveNext());
            }
        }

        [RavenTheory(RavenTestCategory.Voron)]
        [InlineData(1)]
        [InlineData(7)]
        [InlineData(8)]
        [InlineData(9)]
        [InlineData(4_000)]
        [InlineData(5_000)]
        [InlineData(Entries - 1)]
        public void MovesAfterASkipForwardFollowThePathOfTheSkip(long skip)
        {
            using var tx = Env.WriteTransaction();
            var fst = CreateTree(tx);

            using var it = fst.Iterate();
            Assert.True(it.Seek(0));
            Assert.True(it.Skip(skip));
            for (var expected = skip; expected < Entries; expected++)
            {
                Assert.Equal(expected, it.CurrentKey);
                Assert.Equal(expected < Entries - 1, it.MoveNext());
            }

            Assert.True(it.Seek(0));
            Assert.False(it.Skip(Entries));
        }

        [RavenTheory(RavenTestCategory.Voron)]
        [InlineData(1)]
        [InlineData(7)]
        [InlineData(8)]
        [InlineData(9)]
        [InlineData(1_000)]
        [InlineData(2_000)]
        [InlineData(Entries - 1)]
        public void MovesAfterASkipBackwardFollowThePathOfTheSkip(long skip)
        {
            using var tx = Env.WriteTransaction();
            var fst = CreateTree(tx);

            using var it = fst.Iterate();
            Assert.True(it.SeekBackward(Entries - 1));
            Assert.True(it.Skip(-skip));
            for (var expected = Entries - 1 - skip; expected >= 0; expected--)
            {
                Assert.Equal(expected, it.CurrentKey);
                Assert.Equal(expected > 0, it.MovePrev());
            }

            Assert.True(it.SeekBackward(Entries - 1));
            Assert.False(it.Skip(-Entries));
        }

        private static FixedSizeTree CreateTree(Transaction tx)
        {
            var fst = tx.FixedTreeFor("paths", ValueSize);

            Span<byte> value = stackalloc byte[ValueSize];
            using (Slice.From(tx.Allocator, value, out var slice))
            {
                for (long key = 0; key < Entries; key++)
                    fst.Add(key, slice);
            }

            Assert.Equal(3, fst.Depth);
            return fst;
        }
    }
}

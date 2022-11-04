using Sparrow.Server.Collections;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Sparrow
{
    public class SmallSetTests : NoDisposalNeeded
    {
        public SmallSetTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void WeakSetSingleItem()
        {
            var ilSet = new WeakSmallSet<int, long>(128);
            ilSet.Add(10, 10);
            Assert.True(ilSet.TryGetValue(10, out var iv));
            Assert.Equal(10, iv);


            var llSet = new WeakSmallSet<long, long>(128);
            llSet.Add(10, 10);
            Assert.True(llSet.TryGetValue(10, out var lv));
            Assert.Equal(10, lv);
        }

        [Fact]
        public void WeakSetSingleLane()
        {
            var ilSet = new WeakSmallSet<int, long>(128);

            for (int i = 0; i < 8; i++)
                ilSet.Add(i, i);

            for (int i = 0; i < 8; i++)
            {
                Assert.True(ilSet.TryGetValue(i, out var iv));
                Assert.Equal(i, iv);
            }

            var llSet = new WeakSmallSet<long, long>(128);
            for (int i = 0; i < 4; i++)
                llSet.Add(i, i);

            for (int i = 0; i < 4; i++)
            {
                Assert.True(llSet.TryGetValue(i, out var lv));
                Assert.Equal(i, lv);
            }
        }

        [Fact]
        public void WeakSetSingleSmallerThanVector()
        {
            var llSet = new WeakSmallSet<long, long>(3);
            llSet.Add(10, 10);
            Assert.True(llSet.TryGetValue(10, out var v));
            Assert.Equal(10, v);
        }

        [Fact]
        public void WeakSetDuplicateItems()
        {
            var ilSet = new WeakSmallSet<int, long>();
            for (int i = 0; i < 8; i++)
                ilSet.Add(i, i);

            Assert.True(ilSet.TryGetValue(7, out var iv));
            Assert.Equal(7, iv);

            for (int i = 0; i < 8; i++)
                ilSet.Add(7, -1);

            for (int i = 0; i < 7; i++)
            {
                Assert.True(ilSet.TryGetValue(i, out iv));
                Assert.Equal(i, iv);
            }

            Assert.True(ilSet.TryGetValue(7, out iv));
            Assert.Equal(-1, iv);
        }

        [Fact]
        public void WeakSetEviction()
        {
            var ilSet = new WeakSmallSet<int, long>();
            for (int i = 0; i < 8; i++)
                ilSet.Add(i, i);

            for (int i = 0; i < 7; i++)
            {
                Assert.True(ilSet.TryGetValue(i, out var iv));
                Assert.True(iv >= 0);
            }

            for (int i = 0; i < 8; i++)
                ilSet.Add(i + 8, -i);

            for (int i = 0; i < 7; i++)
            {
                Assert.True(ilSet.TryGetValue(i + 8, out var iv));
                Assert.True(iv <= 0);
            }
        }

        [Fact]
        public void WeakMultipleChunks()
        {
            var ilSet = new WeakSmallSet<int, long>(16);
            for (int i = 0; i < 16; i++)
                ilSet.Add(i, i);

            Assert.True(ilSet.TryGetValue(15, out var iv));
            Assert.Equal(15, iv);

            for (int i = 0; i < 16; i++)
            {
                Assert.True(ilSet.TryGetValue(i, out iv));
                Assert.Equal(i, iv);
            }
        }
    }
}

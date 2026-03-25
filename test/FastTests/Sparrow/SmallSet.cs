using System.Numerics;
using Sparrow.Server.Collections;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Sparrow
{
    public class SmallSetTests : NoDisposalNeeded
    {
        public SmallSetTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Memory)]
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

        [RavenFact(RavenTestCategory.Memory)]
        public void WeakSetSingleLane()
        {
            var Ni = Vector<int>.Count;
            var ilSet = new WeakSmallSet<int, long>(32 * Ni);

            for (int i = 0; i < Ni; i++)
                ilSet.Add(i, i);

            for (int i = 0; i < Ni; i++)
            {
                Assert.True(ilSet.TryGetValue(i, out var iv));
                Assert.Equal(i, iv);
            }

            var Nl = Vector<long>.Count;
            var llSet = new WeakSmallSet<long, long>(32 * Nl);
            for (int i = 0; i < Nl; i++)
                llSet.Add(i, i);

            for (int i = 0; i < Nl; i++)
            {
                Assert.True(llSet.TryGetValue(i, out var lv));
                Assert.Equal(i, lv);
            }
        }

        [RavenFact(RavenTestCategory.Memory)]
        public void WeakSetSingleSmallerThanVector()
        {
            var N = Vector<long>.Count;

            var llSet = new WeakSmallSet<long, long>(N-1);
            llSet.Add(10, 10);
            Assert.True(llSet.TryGetValue(10, out var v));
            Assert.Equal(10, v);
        }

        [RavenFact(RavenTestCategory.Memory)]
        public void WeakSetDuplicateItems()
        {
            var N = Vector<int>.Count;

            var ilSet = new WeakSmallSet<int, long>();
            for (int i = 0; i < N; i++)
                ilSet.Add(i, i);

            Assert.True(ilSet.TryGetValue(N - 1, out var iv));
            Assert.Equal(N - 1, iv);

            for (int i = 0; i < N; i++)
                ilSet.Add(N-1, -1);

            for (int i = 0; i < N-1; i++)
            {
                Assert.True(ilSet.TryGetValue(i, out iv));
                Assert.Equal(i, iv);
            }

            Assert.True(ilSet.TryGetValue(N-1, out iv));
            Assert.Equal(-1, iv);
        }

        [RavenFact(RavenTestCategory.Memory)]
        public void WeakSetEviction()
        {
            var N = Vector<int>.Count;

            var ilSet = new WeakSmallSet<int, long>();
            for (int i = 0; i < N; i++)
                ilSet.Add(i, i);

            for (int i = 0; i < N - 1; i++)
            {
                Assert.True(ilSet.TryGetValue(i, out var iv));
                Assert.True(iv >= 0);
            }

            for (int i = 0; i < N; i++)
                ilSet.Add(i + N, -i);

            for (int i = 0; i < N - 1; i++)
            {
                Assert.True(ilSet.TryGetValue(i + N, out var iv));
                Assert.True(iv <= 0);
            }
        }

        [RavenFact(RavenTestCategory.Memory)]
        public void WeakMultipleChunks()
        {
            var N = 2 * Vector<int>.Count;

            var ilSet = new WeakSmallSet<int, long>(N);
            for (int i = 0; i < N; i++)
                ilSet.Add(i, i);

            Assert.True(ilSet.TryGetValue(N - 1, out var iv));
            Assert.Equal(N - 1, iv);

            for (int i = 0; i < N; i++)
            {
                Assert.True(ilSet.TryGetValue(i, out iv));
                Assert.Equal(i, iv);
            }
        }

        [RavenFact(RavenTestCategory.Memory)]
        public void SetSingleItem()
        {
            var N = 2 * Vector<int>.Count;

            var ilSet = new SmallSet<int, long>(N);
            ilSet.Add(10, 10);
            Assert.True(ilSet.TryGetValue(10, out var iv));
            Assert.Equal(10, iv);


            var llSet = new SmallSet<long, long>(N);
            llSet.Add(10, 10);
            Assert.True(llSet.TryGetValue(10, out var lv));
            Assert.Equal(10, lv);
        }

        [RavenFact(RavenTestCategory.Memory)]
        public void SetSingleLane()
        {
            var Ni = Vector<int>.Count;

            var ilSet = new SmallSet<int, long>(32 * Ni);

            for (int i = 0; i < Ni; i++)
                ilSet.Add(i, i);

            for (int i = 0; i < Ni; i++)
            {
                Assert.True(ilSet.TryGetValue(i, out var iv));
                Assert.Equal(i, iv);
            }

            var Nl = Vector<long>.Count;
            var llSet = new SmallSet<long, long>(128);
            for (int i = 0; i < Nl; i++)
                llSet.Add(i, i);

            for (int i = 0; i < Nl; i++)
            {
                Assert.True(llSet.TryGetValue(i, out var lv));
                Assert.Equal(i, lv);
            }
        }

        [RavenFact(RavenTestCategory.Memory)]
        public void SetSingleLaneWithOverflow()
        {
            var Ni = Vector<int>.Count;
            var ilSet = new SmallSet<int, long>(Ni);
            for (int i = 0; i < 2 * Ni; i++)
                ilSet.Add(i, i);

            for (int i = 0; i < 2 * Ni; i++)
            {
                Assert.True(ilSet.TryGetValue(i, out var iv));
                Assert.Equal(i, iv);
            }

            var Nl = Vector<long>.Count;
            var llSet = new SmallSet<long, long>(Nl);
            for (int i = 0; i < 4 * Nl; i++)
                llSet.Add(i, i);

            for (int i = 0; i < 4 * Nl; i++)
            {
                Assert.True(llSet.TryGetValue(i, out var lv));
                Assert.Equal(i, lv);
            }
        }

        [RavenFact(RavenTestCategory.Memory)]
        public void SetDuplicateItems()
        {
            var N = Vector<int>.Count;
            var ilSet = new SmallSet<int, long>();
            for (int i = 0; i < N; i++)
                ilSet.Add(i, i);

            Assert.True(ilSet.TryGetValue(N-1, out var iv));
            Assert.Equal(N-1, iv);

            for (int i = 0; i < N; i++)
                ilSet.Add(N-1, -1);

            for (int i = 0; i < N-1; i++)
            {
                Assert.True(ilSet.TryGetValue(i, out iv));
                Assert.Equal(i, iv);
            }

            Assert.True(ilSet.TryGetValue(N-1, out iv));
            Assert.Equal(-1, iv);
        }
    }
}

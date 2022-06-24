using System;
using System.Collections.Generic;
using System.Linq;
using Tests.Infrastructure;
using Voron;
using Voron.Data.Sets;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Voron.Sets
{
    public class SetTests : StorageTest
    {
        private readonly List<long> _data;
        private readonly List<long> _random;

        public SetTests(ITestOutputHelper output) : base(output)
        {
            const int Size = 400_000;
            var diff = new[] { 17, 250, 4828, 28, 12, 3 };
            var random = new Random(231);
            _data = new List<long>();
            long s = 0;
            for (int i = 0; i < Size; i++)
            {
                s += diff[random.Next(diff.Length)];
                _data.Add(s);
            }

            _random = _data.OrderBy(x => random.Next()).ToList();
        }

        private List<long> AllValues(Set set)
        {
            var it = set.Iterate();
            var l = new List<long>();
            if (it.Seek(0) == false)
                return l;
            while (it.MoveNext())
            {
                l.Add(it.Current);
            }

            return l;
        }

        [Fact]
        public void CanCreateSet()
        {
            using (var wtx = Env.WriteTransaction())
            {
                var tree = wtx.OpenSet("test");
                tree.Add(5);
                wtx.Commit();
            }

            using (var rtx = Env.ReadTransaction())
            {
                var tree = rtx.OpenSet("test");
                var values = AllValues(tree);
                Assert.Equal(new[] { 5L }, values);
            }
        }


        [Fact]
        public void CanCreateSetAndAddLargeValue()
        {
            using (var wtx = Env.WriteTransaction())
            {
                var tree = wtx.OpenSet("test");
                tree.Add(5L + int.MaxValue);
                wtx.Commit();
            }

            using (var rtx = Env.ReadTransaction())
            {
                var tree = rtx.OpenSet("test");
                var values = AllValues(tree);
                Assert.Equal(new[] { 5L + int.MaxValue }, values);
            }
        }


        [Fact]
        public void CanDeleteItem()
        {
            using (var wtx = Env.WriteTransaction())
            {
                var tree = wtx.OpenSet("test");
                tree.Add(5);
                wtx.Commit();
            }

            using (var wtx = Env.WriteTransaction())
            {
                var tree = wtx.OpenSet("test");
                tree.Remove(5);
                wtx.Commit();
            }

            using (var rtx = Env.ReadTransaction())
            {
                var tree = rtx.OpenSet("test");
                Assert.Empty(AllValues(tree));
            }
        }


        [Fact]
        public void CanStoreLargeNumberOfItemsInRandomlyOrder()
        {
            using (var wtx = Env.WriteTransaction())
            {
                var tree = wtx.OpenSet("test");
                foreach (long i in _random)
                {
                    tree.Add(i);
                }

                wtx.Commit();
            }

            using (var rtx = Env.ReadTransaction())
            {
                var tree = rtx.OpenSet("test");
                List<long> allValues = AllValues(tree);
                IEnumerable<long> diff = _data.Except(allValues).ToArray();
                Assert.Empty(diff);
                Assert.Equal(_data, allValues);
            }
        }


        [Fact]
        public void CanDeleteLargeNumberOfItems()
        {
            using (var wtx = Env.WriteTransaction())
            {
                var tree = wtx.OpenSet("test");
                foreach (long i in _random)
                {
                    tree.Add(i);
                }

                wtx.Commit();
            }

            using (var wtx = Env.WriteTransaction())
            {
                var tree = wtx.OpenSet("test");
                foreach (long i in _random)
                {
                    tree.Remove(i);
                }

                wtx.Commit();
            }

            using (var rtx = Env.ReadTransaction())
            {
                var tree = rtx.OpenSet("test");
                Assert.Empty(AllValues(tree));
                Assert.Equal(0, tree.State.BranchPages);
                Assert.Equal(1, tree.State.LeafPages);
                Assert.Equal(1, tree.State.Depth);
            }
        }


        [Fact]
        public void CanDeleteRandomLargeNumberOfItemsFromStart()
        {
            using (var wtx = Env.WriteTransaction())
            {
                var tree = wtx.OpenSet("test");
                foreach (long i in _data)
                {
                    tree.Add(i);
                }

                wtx.Commit();
            }

            using (var wtx = Env.WriteTransaction())
            {
                var tree = wtx.OpenSet("test");
                foreach (long i in _random)
                {
                    tree.Remove(i);
                }

                wtx.Commit();
            }

            using (var rtx = Env.ReadTransaction())
            {
                var tree = rtx.OpenSet("test");
                Assert.Empty(AllValues(tree));
                Assert.Equal(0, tree.State.BranchPages);
                Assert.Equal(1, tree.State.LeafPages);
                Assert.Equal(1, tree.State.Depth);
            }
        }

        [Fact]
        public void CanAddPredictableOffsets_Large()
        {
            using (var wtx = Env.WriteTransaction())
            {
                var tree = wtx.OpenSet("test");

                for (int i = 0; i < 10_000; i++)
                {
                    var offset = (i + 100) * 8192;
                    for (int j = 0; j < 128; j++)
                    {
                        offset += 2;
                        tree.Add(offset);
                    }
                }

                wtx.Commit();
            }

            using (var rtx = Env.ReadTransaction())
            {
                var tree = rtx.OpenSet("test");
                var it = tree.Iterate();
                Assert.True(it.Seek(0));
                bool movedNext = true;
                for (int i = 0; i < 10_000; i++)
                {
                    var offset = (i + 100) * 8192;
                    for (int j = 0; j < 128; j++)
                    {
                        offset += 2;
                        movedNext = it.MoveNext();
                        Assert.True(movedNext);
                        Assert.Equal(offset, it.Current);
                    }
                }

                Assert.False(it.MoveNext());
            }
        }

        [Fact]
        public void CanDeletesSmallNumberOfItems()
        {
            int count = 3213 * 2;
            using (var wtx = Env.WriteTransaction())
            {
                var tree = wtx.OpenSet("test");
                foreach (long i in _random.Take(count))
                {
                    tree.Add(i);
                }

                wtx.Commit();
            }

            using (var wtx = Env.WriteTransaction())
            {
                var tree = wtx.OpenSet("test");
                foreach (long i in _random.Take(count))
                {
                    tree.Remove(i);
                }

                wtx.Commit();
            }

            using (var rtx = Env.ReadTransaction())
            {
                var tree = rtx.OpenSet("test");
                Assert.Empty(AllValues(tree));
                Assert.Equal(0, tree.State.BranchPages);
                Assert.Equal(1, tree.State.LeafPages);
                Assert.Equal(1, tree.State.Depth);
            }
        }


        [Fact]
        public void CanDeleteLargeNumberOfItemsFromStart()
        {
            using (var wtx = Env.WriteTransaction())
            {
                var tree = wtx.OpenSet("test");
                foreach (long i in _data)
                {
                    tree.Add(i);
                }

                wtx.Commit();
            }

            using (var wtx = Env.WriteTransaction())
            {
                var tree = wtx.OpenSet("test");
                foreach (long i in _data)
                {
                    tree.Remove(i);
                }

                wtx.Commit();
            }

            using (var rtx = Env.ReadTransaction())
            {
                var tree = rtx.OpenSet("test");
                Assert.Empty(AllValues(tree));
                Assert.Equal(0, tree.State.BranchPages);
                Assert.Equal(1, tree.State.LeafPages);
                Assert.Equal(1, tree.State.Depth);
            }
        }


        [Fact]
        public void CanDeleteLargeNumberOfItemsFromEnd()
        {
            using (var wtx = Env.WriteTransaction())
            {
                var tree = wtx.OpenSet("test");
                foreach (long i in _data)
                {
                    tree.Add(i);
                }

                wtx.Commit();
            }

            using (var wtx = Env.WriteTransaction())
            {
                var tree = wtx.OpenSet("test");
                for (int i = _data.Count - 1; i >= 0; i--)
                {
                    tree.Remove(_data[i]);
                }

                wtx.Commit();
            }

            using (var rtx = Env.ReadTransaction())
            {
                var tree = rtx.OpenSet("test");
                Assert.Empty(AllValues(tree));
                Assert.Equal(0, tree.State.BranchPages);
                Assert.Equal(1, tree.State.LeafPages);
                Assert.Equal(1, tree.State.Depth);
            }
        }


        [Fact]
        public void CanStoreLargeNumberOfItemsInSequentialOrder()
        {
            using (var wtx = Env.WriteTransaction())
            {
                var tree = wtx.OpenSet("test");
                foreach (long i in _data)
                {
                    tree.Add(i);
                }

                wtx.Commit();
            }

            using (var rtx = Env.ReadTransaction())
            {
                var tree = rtx.OpenSet("test");
                Assert.Equal(_data, AllValues(tree));
            }
        }

        [RavenTheory(RavenTestCategory.Voron)]
        [InlineData(1337, 200000)]
        [InlineData(73014, 35)]
        public void CanDeleteAndInsertInRandomOrder(int seed, int size)
        {
            static void Shuffle(int[] list, Random rng)
            {
                int n = list.Length;
                while (n > 1)
                {
                    n--;
                    int k = rng.Next(n + 1);
                    (list[k], list[n]) = (list[n], list[k]);
                }
            }

            Random random = new Random(seed);

            var uniqueKeys = new HashSet<int>();
            var inTreeKeys = new HashSet<int>();
            var removedKeys = new HashSet<int>();

            int name = random.Next();

            for (int iter = 0; iter < 4; iter++)
            {
                using (var wtx = Env.WriteTransaction())
                {
                    var set = wtx.OpenSet($"Set({name})");
                    for (int i = 0; i < size; i++)
                    {
                        var rname = (int)(uint)random.Next();
                        if (!uniqueKeys.Contains(rname))
                        {
                            uniqueKeys.Add(rname);
                            inTreeKeys.Add(rname);
                            set.Add(rname);
                        }
                    }

                    Assert.Equal(inTreeKeys.Count, set.State.NumberOfEntries);

                    wtx.Commit();
                }

                var values = inTreeKeys.ToArray();
                Shuffle(values, random);

                using (var wtx = Env.WriteTransaction())
                {
                    var set = wtx.OpenSet($"Set({name})");
                    for (int i = 0; i < size / 2; i++)
                    {
                        set.Remove(values[i]);
                        inTreeKeys.Remove(values[i]);
                        removedKeys.Add(values[i]);

                        Assert.Equal(inTreeKeys.Count, set.State.NumberOfEntries);
                    }

                    wtx.Commit();
                }

                using (var rtx = Env.ReadTransaction())
                {
                    var matches = new long[size * 4];
                    var set = rtx.OpenSet($"Set({name})");
                    set.Iterate().Fill(matches, out int read);
                    Assert.Equal(inTreeKeys.Count, read);
                    for (int i = 0; i < read; i++)
                    {
                        Assert.True(inTreeKeys.TryGetValue((int)matches[i], out var _));
                    }
                }
            }
        }
    }
}

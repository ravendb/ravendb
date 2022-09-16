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
        public void CanAddHugeOffsets_Large()
        {
            HashSet<long> valuesInSet = new();

            using (var wtx = Env.WriteTransaction())
            {
                var tree = wtx.OpenSet("test");

                Span<long> buffer = stackalloc long[16];

                Random rnd = new(1337);
                for (int i = 0; i < 100; i++)
                {
                    int j = 0;
                    while (j < buffer.Length)
                    {
                        long valueToAdd = (long)rnd.Next(10) * int.MaxValue + rnd.Next(100000);
                        if (!valuesInSet.Contains(valueToAdd))
                        {
                            buffer[j] = valueToAdd;
                            valuesInSet.Add(valueToAdd);
                            j++;
                        }
                    }

                    var valuesToAdd = buffer.Slice(0, j);
                    valuesToAdd.Sort();

                    tree.Add(valuesToAdd);
                }

                wtx.Commit();
            }

            using (var rtx = Env.ReadTransaction())
            {
                var tree = rtx.OpenSet("test");
                var it = tree.DumpAllValues();
                
                foreach( long item in valuesInSet)
                    Assert.True(it.Contains(item));
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
        
        [RavenTheory(RavenTestCategory.Voron)]
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

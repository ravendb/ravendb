using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Tests.Infrastructure;
using Voron;
using Voron.Data.PostingLists;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Voron.Sets
{
    public class PostingListTests : StorageTest
    {
        private readonly List<long> _data;
        private readonly List<long> _random;

        public PostingListTests(ITestOutputHelper output) : base(output)
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

        private List<long> AllValues(PostingList postingList)
        {
            var it = postingList.Iterate();
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
                var tree = wtx.OpenPostingList("test");
                tree.Add(5);
                wtx.Commit();
            }

            using (var rtx = Env.ReadTransaction())
            {
                var tree = rtx.OpenPostingList("test");
                var values = AllValues(tree);
                Assert.Equal(new[] { 5L }, values);
            }
        }


        [Fact]
        public void CanCreateSetAndAddLargeValue()
        {
            using (var wtx = Env.WriteTransaction())
            {
                var tree = wtx.OpenPostingList("test");
                tree.Add(5L + int.MaxValue);
                wtx.Commit();
            }

            using (var rtx = Env.ReadTransaction())
            {
                var tree = rtx.OpenPostingList("test");
                var values = AllValues(tree);
                Assert.Equal(new[] { 5L + int.MaxValue }, values);
            }
        }


        [Fact]
        public void CanDeleteItem()
        {
            using (var wtx = Env.WriteTransaction())
            {
                var tree = wtx.OpenPostingList("test");
                tree.Add(5);
                wtx.Commit();
            }

            using (var wtx = Env.WriteTransaction())
            {
                var tree = wtx.OpenPostingList("test");
                tree.Remove(5);
                wtx.Commit();
            }

            using (var rtx = Env.ReadTransaction())
            {
                var tree = rtx.OpenPostingList("test");
                Assert.Empty(AllValues(tree));
            }
        }
        
        [Fact]
        public void CanAddHugeOffsets_Large()
        {
            HashSet<long> valuesInSet = new();

            using (var wtx = Env.WriteTransaction())
            {
                var tree = wtx.OpenPostingList("test");

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
                var tree = rtx.OpenPostingList("test");
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
                var tree = wtx.OpenPostingList("test");
                foreach (long i in _random.Take(count))
                {
                    tree.Add(i);
                }

                wtx.Commit();
            }

            using (var wtx = Env.WriteTransaction())
            {
                var tree = wtx.OpenPostingList("test");
                foreach (long i in _random.Take(count))
                {
                    tree.Remove(i);
                }

                wtx.Commit();
            }

            using (var rtx = Env.ReadTransaction())
            {
                var tree = rtx.OpenPostingList("test");
                Assert.Empty(AllValues(tree));
                Assert.Equal(0, tree.State.BranchPages);
                Assert.Equal(1, tree.State.LeafPages);
                Assert.Equal(1, tree.State.Depth);
            }
        }
        
        [RavenTheory(RavenTestCategory.Voron)]
        [InlineData(73014, 35)]
        public void CanDeleteAndInsertInRandomOrder(int seed, int size, int iterations = 4)
        {
            static void Shuffle<T>(T[] list, Random rng)
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

            var uniqueKeys = new HashSet<long>();
            var inTreeKeys = new HashSet<long>();
            var removedKeys = new HashSet<long>();

            int name = random.Next();

            for (int iter = 0; iter < iterations; iter++)
            {
                using (var wtx = Env.WriteTransaction())
                {
                    var set = wtx.OpenPostingList($"Set({name})");
                    for (int i = 0; i < size; i++)
                    {
                        var rname = (int)(uint)random.Next();
                        if (uniqueKeys.Add(rname))
                        {
                            inTreeKeys.Add(rname);
                            set.Add(rname);
                        }
                    }


                    wtx.Commit();
                }
                
                using (var rtx = Env.ReadTransaction())
                {
                    var matches = new long[inTreeKeys.Count];
                    var set = rtx.OpenPostingList($"Set({name})");

                    set.Iterate().Fill(matches, out int read);
                    Assert.Equal(inTreeKeys.Count, read);
                    for (int i = 0; i < read; i++)
                    {
                        Assert.True(inTreeKeys.TryGetValue(matches[i], out var _), "Missing " + matches[i]);
                    }
             
                    Assert.Equal(inTreeKeys.Count, set.State.NumberOfEntries);
                }

                var values = inTreeKeys.ToArray();
                Shuffle(values, random);

                using (var wtx = Env.WriteTransaction())
                {
                    var set = wtx.OpenPostingList($"Set({name})");
                    for (int i = 0; i < size / 2; i++)
                    {
                        set.Remove(values[i]);
                        inTreeKeys.Remove(values[i]);
                        removedKeys.Add(values[i]);
                    }

                    wtx.Commit();
                }

                using (var rtx = Env.ReadTransaction())
                {
                    var matches = new long[inTreeKeys.Count];
                    var set = rtx.OpenPostingList($"Set({name})");
                    set.Iterate().Fill(matches, out int read);
                    Assert.Equal(inTreeKeys.Count, read);
                    for (int i = 0; i < read; i++)
                    {
                        Assert.True(inTreeKeys.TryGetValue(matches[i], out var _), "Missing " + matches[i]);
                    }
                }
            }
        }
    }
}

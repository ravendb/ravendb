using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Sparrow.Server.Debugging;
using Voron.Data.CompactTrees;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Voron
{
    public class CompactTreeTests : StorageTest
    {
        public CompactTreeTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void TrickyAttempts()
        {
            using (var wtx = Env.WriteTransaction())
            {
                var tree = CompactTree.Create(wtx.LowLevelTransaction, "test");
                tree.Add("Pipeline1", 4);
                tree.Add("Pipeline2", 5);
                tree.Add("Pipeline3", 5);
                wtx.Commit();
            }
            
            using (var wtx = Env.WriteTransaction())
            {
                var tree = CompactTree.Create(wtx.LowLevelTransaction, "test");
                tree.Add("Pipeline2", 1007);
            
                Assert.True(tree.TryGetValue("Pipeline1", out var r));
                Assert.Equal(4, r);
                Assert.True(tree.TryGetValue("Pipeline2", out  r));
                Assert.Equal(1007, r);
                
                Assert.True(tree.TryGetValue("Pipeline3", out  r));
                Assert.Equal(5, r);
            }
        }
        [Fact]
        public void CanCreateCompactTree()
        {
            using (var wtx = Env.WriteTransaction())
            {
                var tree = CompactTree.Create(wtx.LowLevelTransaction, "test");
                tree.Add("hi", 5);
                wtx.Commit();
            }
            using (var rtx = Env.ReadTransaction())
            {
                var tree = CompactTree.Create(rtx.LowLevelTransaction, "test");
                Assert.True(tree.TryGetValue("hi", out var r));
                Assert.Equal(5, r);
            }
        }

        [Fact]
        public void CanFindElementInSinglePage()
        {
            using (var wtx = Env.WriteTransaction())
            {
                var tree = CompactTree.Create(wtx.LowLevelTransaction, "test");
                tree.Add("hi/10", 5);
                tree.Add("hi/11", 6);
                tree.Add("hi/12", 7);
                wtx.Commit();
            }
            using (var rtx = Env.ReadTransaction())
            {
                var tree = CompactTree.Create(rtx.LowLevelTransaction, "test");
                Assert.True(tree.TryGetValue("hi/10", out var r));
                Assert.Equal(5, r);
                Assert.True(tree.TryGetValue("hi/11", out r));
                Assert.Equal(6, r);
                Assert.True(tree.TryGetValue("hi/12", out r));
                Assert.Equal(7, r);
            }
        }


        [Fact]
        public void CanHandleVeryLargeKey()
        {
            using (var wtx = Env.WriteTransaction())
            {
                var tree = CompactTree.Create(wtx.LowLevelTransaction, "test");
                tree.Add(new string('a', 577), 5);
                wtx.Commit();
            }
            using (var rtx = Env.ReadTransaction())
            {
                var tree = CompactTree.Create(rtx.LowLevelTransaction, "test");
                Assert.True(tree.TryGetValue(new string('a', 577), out var r));
                Assert.Equal(5, r);
            }
        }
        
        [Fact]
        public void CanDeleteItem()
        {
            using (var wtx = Env.WriteTransaction())
            {
                var tree = CompactTree.Create(wtx.LowLevelTransaction, "test");
                tree.Add("hi", 5);
                wtx.Commit();
            }
            
            using (var wtx = Env.WriteTransaction())
            {
                var tree = CompactTree.Create(wtx.LowLevelTransaction, "test");
                Assert.True(tree.TryRemove("hi", out var r));
                Assert.Equal(5, r);
                wtx.Commit();
            }
            using (var rtx = Env.ReadTransaction())
            {
                var tree = CompactTree.Create(rtx.LowLevelTransaction, "test");
                Assert.False(tree.TryGetValue("hi", out var r));
            }
        }
        
        
        [Fact]
        public void CanStoreLargeNumberOfItemsInRandomlyOrder()
        {
            const int Size = 400000;

            using (var wtx = Env.WriteTransaction())
            {
                var tree = CompactTree.Create(wtx.LowLevelTransaction, "test");
                for (int i = 0; i < Size; i++)
                {
                    tree.Add("hi" + i, i);
                }
                wtx.Commit();
            }
            using (var rtx = Env.ReadTransaction())
            {
                var tree = CompactTree.Create(rtx.LowLevelTransaction, "test");
                for (int i = 0; i < Size; i++)
                {
                    Assert.True(tree.TryGetValue("hi" + i, out var r));
                    Assert.Equal(i, r);
                }
            }
        }


        [Fact]
        public void CanDeleteLargeNumberOfItemsInRandomOrder()
        {
            const int Size = 400000;

            using (var wtx = Env.WriteTransaction())
            {
                var tree = CompactTree.Create(wtx.LowLevelTransaction, "test");
                for (int i = 0; i < Size; i++)
                {
                    tree.Add("hi" + i, i);
                }
                wtx.Commit();
            }

            using (var wtx = Env.WriteTransaction())
            {
                var tree = CompactTree.Create(wtx.LowLevelTransaction, "test");
                for (int i = 0; i < Size; i++)
                {
                    Assert.True(tree.TryRemove("hi" + i, out var v));
                    Assert.Equal(i, v);
                }
                wtx.Commit();
            }
            using (var rtx = Env.ReadTransaction())
            {
                var tree = CompactTree.Create(rtx.LowLevelTransaction, "test");
                Assert.Equal(0, tree.State.NumberOfEntries);
                Assert.Equal(0, tree.State.BranchPages);
                Assert.Equal(1, tree.State.LeafPages);
                Assert.Equal(1, tree.State.Depth);
            }
        }


        [Fact]
        public void CanDeleteLargeNumberOfItemsFromStart()
        {
            const int Size = 400000;
            using (var wtx = Env.WriteTransaction())
            {
                var tree = CompactTree.Create(wtx.LowLevelTransaction, "test");
                for (int i = 0; i < Size; i++)
                {
                    tree.Add($"hi{i:00000000}", i);
                }
                wtx.Commit();
            }
            
            using (var wtx = Env.WriteTransaction())
            {
                var tree = CompactTree.Create(wtx.LowLevelTransaction, "test");
                for (int i = 0; i < Size; i++)
                {
                    Assert.True(tree.TryRemove($"hi{i:00000000}", out var v));
                    Assert.Equal(i, v);
                }
                wtx.Commit();
            }
            using (var rtx = Env.ReadTransaction())
            {
                var tree = CompactTree.Create(rtx.LowLevelTransaction, "test");
                Assert.Equal(0, tree.State.NumberOfEntries);
                Assert.Equal(0, tree.State.BranchPages);
                Assert.Equal(1, tree.State.LeafPages);
                Assert.Equal(1, tree.State.Depth);
            }
        }
        
              
        [Fact]
        public void CanDeleteLargeNumberOfItemsFromEnd()
        {
            const int Size = 400000;
            using (var wtx = Env.WriteTransaction())
            {
                var tree = CompactTree.Create(wtx.LowLevelTransaction, "test");
                for (int i = 0; i < Size; i++)
                {
                    tree.Add($"hi{i:00000000}", i);
                }
                wtx.Commit();
            }
            
            using (var wtx = Env.WriteTransaction())
            {
                var tree = CompactTree.Create(wtx.LowLevelTransaction, "test");
                for (int i = Size - 1; i >= 0; i--)
                {
                    Assert.True(tree.TryRemove($"hi{i:00000000}", out var v));
                    Assert.Equal(i, v);
                }
                wtx.Commit();
            }
            using (var rtx = Env.ReadTransaction())
            {
                var tree = CompactTree.Create(rtx.LowLevelTransaction, "test");
                Assert.Equal(0, tree.State.NumberOfEntries);
                Assert.Equal(0, tree.State.BranchPages);
                Assert.Equal(1, tree.State.LeafPages);
                Assert.Equal(1, tree.State.Depth);
            }
        }


        [Fact]
        public void CanStoreLargeNumberOfItemsInSequentialOrder()
        {
            const int Size = 400000;
            using (var wtx = Env.WriteTransaction())
            {
                var tree = CompactTree.Create(wtx.LowLevelTransaction, "test");
                for (int i = 0; i < Size; i++)
                {
                    tree.Add($"hi{i:00000000}", i);
                }
                wtx.Commit();
            }
            using (var rtx = Env.ReadTransaction())
            {
                var tree = CompactTree.Create(rtx.LowLevelTransaction, "test");
                for (int i = 0; i < Size; i++)
                {
                    var result = tree.TryGetValue($"hi{i:00000000}", out var r);
                    Assert.True(result);
                    Assert.Equal(i, r);
                }
            }
        }


        [Fact]
        public void CanRecompressItemsWithDeletesAndInserts()
        {
            static void Shuffle(string[] list, Random rng)
            {
                int n = list.Length;
                while (n > 1)
                {
                    n--;
                    int k = rng.Next(n + 1);
                    var value = list[k];
                    list[k] = list[n];
                    list[n] = value;
                }
            }

            const int Size = 200000;

            Random random = new Random(1337);

            var uniqueKeys = new HashSet<string>();
            var inTreeKeys = new HashSet<string>();
            var removedKeys = new HashSet<string>();

            for ( int iter = 0; iter < 4; iter++ )
            {
                using (var wtx = Env.WriteTransaction())
                {
                    var tree = CompactTree.Create(wtx.LowLevelTransaction, "test");
                    for (int i = 0; i < Size; i++)
                    {
                        var rname = random.Next();
                        var key = "hi" + rname;
                        if (!uniqueKeys.Contains(key))
                        {
                            uniqueKeys.Add(key);
                            inTreeKeys.Add(key);
                            tree.Add(key, rname);
                        }
                    }

                    tree.TryImproveDictionaryByFullScanning();
                    wtx.Commit();
                }

                var values = inTreeKeys.ToArray();
                Shuffle(values, random);

                using (var wtx = Env.WriteTransaction())
                {
                    var tree = CompactTree.Create(wtx.LowLevelTransaction, "test");
                    for (int i = 0; i < Size / 2; i++)
                    {                        
                        Assert.True(tree.TryRemove(values[i], out var v));
                        inTreeKeys.Remove(values[i]);
                        removedKeys.Add(values[i]);
                    }
                    wtx.Commit();
                }
            }

            using (var rtx = Env.ReadTransaction())
            {
                var tree = CompactTree.Create(rtx.LowLevelTransaction, "test");
                Assert.Equal(inTreeKeys.Count, tree.State.NumberOfEntries);
                Assert.True(inTreeKeys.Count <= tree.State.NextTrainAt);


                foreach (var key in inTreeKeys)
                    Assert.True(tree.TryGetValue(key, out var v));

                foreach (var key in removedKeys)
                    Assert.False(tree.TryGetValue(key, out var v));
            }
        }
    }
}

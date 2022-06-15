using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Sparrow.Server.Debugging;
using Tests.Infrastructure;
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

        [RavenFact(RavenTestCategory.Voron)]
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
        
        [RavenFact(RavenTestCategory.Voron)]
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

        [RavenFact(RavenTestCategory.Voron)]
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


        [RavenFact(RavenTestCategory.Voron)]
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

        [RavenFact(RavenTestCategory.Voron)]
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


        [RavenFact(RavenTestCategory.Voron)]
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


        [RavenFact(RavenTestCategory.Voron)]
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
            }
        }

        [RavenTheory(RavenTestCategory.Voron)]
        [InlineData(400000)]
        public void CanDeleteLargeNumberOfItemsInRandomInsertionOrder(int size, int random = 1337)
        {
            HashSet<int> keys = new HashSet<int>();
            Random rnd = new Random(random);
            using (var wtx = Env.WriteTransaction())
            {
                var tree = CompactTree.Create(wtx.LowLevelTransaction, "test");
                for (int i = 0; i < size; i++)
                {
                    int k = rnd.Next();
                    if (!keys.Contains(k))
                    {
                        tree.Add("hi" + k, i);
                        keys.Add(k);
                    }
                }
                wtx.Commit();                
            }

            keys = new HashSet<int>();
            rnd = new Random(random);
            using (var wtx = Env.WriteTransaction())
            {
                var tree = CompactTree.Create(wtx.LowLevelTransaction, "test");
                for (int i = 0; i < size; i++)
                {
                    int k = rnd.Next();
                    if (!keys.Contains(k))
                    {
                        Assert.True(tree.TryRemove("hi" + k, out var v));
                        Assert.Equal(i, v);

                        keys.Add(k);
                    }
                }
                wtx.Commit();
            }

            using (var rtx = Env.ReadTransaction())
            {
                var tree = CompactTree.Create(rtx.LowLevelTransaction, "test");
                Assert.Equal(0, tree.State.NumberOfEntries);
                Assert.Equal(0, tree.State.BranchPages);
                Assert.Equal(1, tree.State.LeafPages);
            }
        }


        [RavenFact(RavenTestCategory.Voron)]
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
            }
        }


        [RavenFact(RavenTestCategory.Voron)]
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
            }
        }


        [RavenFact(RavenTestCategory.Voron)]
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
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
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
                    Assert.True(tree.TryGetValue("hi"+ i, out var r));
                    Assert.Equal(i, r);
                }
            }
        }
        
        
        [Fact]
        public void CanDeleteLargeNumberOfItems()
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
                    Assert.True(tree.TryGetValue($"hi{i:00000000}", out var r));
                    Assert.Equal(i, r);
                }
            }
        }
    }
}

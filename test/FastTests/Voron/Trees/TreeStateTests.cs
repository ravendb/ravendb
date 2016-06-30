// -----------------------------------------------------------------------
//  <copyright file="TreeStateTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using Xunit;
using Voron;
using Voron.Global;

namespace FastTests.Voron.Trees
{
    public class TreeStateTests : StorageTest
    {
        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.PageSize = 4 * Constants.Size.Kilobyte;
        }

        [Theory]
        [InlineData(5, 2)]
        [InlineData(35, 13)]
        [InlineData(256, 32)]
        public void TotalPageCountConsistsOfLeafBrancheAndOverflowPages(int regularItemsCount, int overflowsCount)
        {
            var r = new Random();

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                for (int i = 0; i < regularItemsCount; i++)
                {
                    tree.Add("test" + new string('-', r.Next(128)) + i, new byte[r.Next(512)]);
                }

                for (int i = 0; i < overflowsCount; i++)
                {
                    tree.Add("overflow" + new string('-', r.Next(128)) + i, new byte[r.Next(8192)]);
                }

                tx.Commit();

                var treeState = tree.State;

                Assert.True(treeState.PageCount > 0);
                Assert.Equal(treeState.PageCount, treeState.BranchPages + treeState.LeafPages + treeState.OverflowPages);
            }
        }

        [Fact]
        public void HasReducedNumberOfPagesAfterRemovingHalfOfEntries()
        {
            const int numberOfRegularItems = 256;
            const int numberOfOverflowItems = 3;

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                for (int i = 0; i < numberOfRegularItems; i++)
                {
                    tree.Add("test" + new string('-', 128) + i, new byte[256]);
                }

                for (int i = 0; i < numberOfOverflowItems; i++)
                {
                    tree.Add("overflow" + new string('-', 128) + i, new byte[8192]);
                }

                tx.Commit();

                Assert.Equal(50, tree.State.PageCount);
                Assert.Equal(38, tree.State.LeafPages);
                Assert.Equal(3, tree.State.BranchPages);
                Assert.Equal(9, tree.State.OverflowPages);
                Assert.Equal(3, tree.State.Depth);				
            }

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                for (int i = 0; i < numberOfRegularItems / 2; i++)
                {
                    tree.Delete("test" + new string('-', 128) + i);
                }

                tree.Delete("overflow" + new string('-', 128) + 0);

                tx.Commit();


                Assert.Equal(31, tree.AllPages().Count);
                Assert.Equal(31, tree.State.PageCount);
                Assert.Equal(22, tree.State.LeafPages);
                Assert.Equal(3, tree.State.BranchPages);
                Assert.Equal(6, tree.State.OverflowPages);
                Assert.Equal(3, tree.State.Depth);

            }
        }

        [Fact]
        public void HasReducedTreeDepthValueAfterRemovingEntries()
        {
            const int numberOfItems = 1024;

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("test");

                for (int i = 0; i < numberOfItems; i++)
                {
                    tree.Add("test" + new string('-', 256) + i, new byte[256]);
                }

                Assert.Equal(4, tree.State.Depth);

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.ReadTree("test");

                for (int i = 0; i < numberOfItems * 0.75; i++)
                {
                    tree.Delete("test" + new string('-', 256) + i);
                }

                Assert.Equal(3, tree.State.Depth);

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.ReadTree("test");

                for (int i = 0; i < numberOfItems; i++)
                {
                    tree.Delete("test" + new string('-', 256) + i);
                }

                Assert.Equal(1, tree.State.Depth);
            }
        }

        [Fact]
        public void AllPagesCantHasDuplicatesInMultiTrees()
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("multi-tree");

                for (int i = 0; i < 100; i++)
                {
                    tree.MultiAdd("key", "items/" + i + "/" + new string('p', i));
                }

                var allPages = tree.AllPages();
                var allPagesDistinct = allPages.Distinct().ToList();
                
                Assert.Equal(allPagesDistinct.Count, allPages.Count);
            }
        }

        [Fact]
        public void MustNotProduceNegativePageCountNumber()
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("multi-tree");

                for (int i = 0; i < 50; i++)
                {
                    tree.MultiAdd("key", "items/" + i + "/" + new string('p', i));
                    tree.MultiAdd("key2", "items/" + i + "/" + new string('p', i));
                }

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.ReadTree("multi-tree");

                for (int i = 0; i < 50; i++)
                {
                    tree.MultiDelete("key", "items/" + i + "/" + new string('p', i));
                    tree.MultiDelete("key2", "items/" + i + "/" + new string('p', i));
                }

                Assert.True(tree.State.PageCount >= 0);
                Assert.Equal(tree.AllPages().Count, tree.State.PageCount);
            }
        }
    }
}

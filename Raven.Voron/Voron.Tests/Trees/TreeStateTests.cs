// -----------------------------------------------------------------------
//  <copyright file="TreeStateTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using Voron.Debugging;
using Voron.Trees;
using Xunit;
using Xunit.Extensions;

namespace Voron.Tests.Trees
{
    public class TreeStateTests : StorageTest
    {
        [Theory]
        [InlineData(5, 2)]
        [InlineData(35, 13)]
        [InlineData(256, 32)]
        public void TotalPageCountConsistsOfLeafBrancheAndOverflowPages(int regularItemsCount, int overflowsCount)
        {
            var r = new Random();

            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                for (int i = 0; i < regularItemsCount; i++)
                {
                    tx.Root.Add("test" + new string('-', r.Next(128)) + i, new byte[r.Next(512)]);
                }

                for (int i = 0; i < overflowsCount; i++)
                {
                    tx.Root.Add("overflow" + new string('-', r.Next(128)) + i, new byte[r.Next(8192)]);
                }

                tx.Commit();

                var treeState = tx.Root.State;

                Assert.True(treeState.PageCount > 0);
                Assert.Equal(treeState.PageCount, treeState.BranchPages + treeState.LeafPages + treeState.OverflowPages);
            }
        }

        [Fact]
        public void HasReducedNumberOfPagesAfterRemovingHalfOfEntries()
        {
            const int numberOfRegularItems = 256;
            const int numberOfOverflowItems = 3;

            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                for (int i = 0; i < numberOfRegularItems; i++)
                {
                    tx.Root.Add("test" + new string('-', 128) + i, new byte[256]);
                }

                for (int i = 0; i < numberOfOverflowItems; i++)
                {
                    tx.Root.Add("overflow" + new string('-', 128) + i, new byte[8192]);
                }

                tx.Commit();

                Assert.Equal(50, tx.Root.State.PageCount);
                Assert.Equal(38, tx.Root.State.LeafPages);
                Assert.Equal(3, tx.Root.State.BranchPages);
                Assert.Equal(9, tx.Root.State.OverflowPages);
                Assert.Equal(3, tx.Root.State.Depth);
            }

            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                for (int i = 0; i < numberOfRegularItems / 2; i++)
                {
                    tx.Root.Delete("test" + new string('-', 128) + i);
                }

                tx.Root.Delete("overflow" + new string('-', 128) + 0);

                tx.Commit();

                DebugStuff.RenderAndShow(tx.Root);

                Assert.Equal(31, tx.Root.AllPages().Count);
                Assert.Equal(31, tx.Root.State.PageCount);
                Assert.Equal(22, tx.Root.State.LeafPages);
                Assert.Equal(3, tx.Root.State.BranchPages);
                Assert.Equal(6, tx.Root.State.OverflowPages);
                Assert.Equal(3, tx.Root.State.Depth);

            }
        }

        [Fact]
        public void HasReducedTreeDepthValueAfterRemovingEntries()
        {
            const int numberOfItems = 1024;

            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var tree = Env.CreateTree(tx, "test");

                for (int i = 0; i < numberOfItems; i++)
                {
                    tree.Add("test" + new string('-', 256) + i, new byte[256]);
                }

                DebugStuff.RenderAndShow(tx, tree.State.RootPageNumber);
                
                Assert.Equal(4, tree.State.Depth);

                tx.Commit();
            }

            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var tree = tx.ReadTree("test");

                for (int i = 0; i < numberOfItems * 0.75; i++)
                {
                    tree.Delete("test" + new string('-', 256) + i);
                }

                DebugStuff.RenderAndShow(tx, tree.State.RootPageNumber);

                Assert.Equal(3, tree.State.Depth);

                tx.Commit();
            }

            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var tree = tx.ReadTree("test");

                for (int i = 0; i < numberOfItems; i++)
                {
                    tree.Delete("test" + new string('-', 256) + i);
                }

                DebugStuff.RenderAndShow(tx, tree.State.RootPageNumber);

                Assert.Equal(1, tree.State.Depth);
            }
        }

        [Fact]
        public void AllPagesCantHasDuplicatesInMultiTrees()
        {
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var tree = Env.CreateTree(tx, "multi-tree");

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
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var tree = Env.CreateTree(tx, "multi-tree");

                for (int i = 0; i < 50; i++)
                {
                    tree.MultiAdd("key", "items/" + i + "/" + new string('p', i));
                    tree.MultiAdd("key2", "items/" + i + "/" + new string('p', i));
                }

                tx.Commit();
            }

            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var tree = tx.ReadTree("multi-tree");

                for (int i = 0; i < 50; i++)
                {
                    Assert.DoesNotThrow(() => tree.MultiDelete("key", "items/" + i + "/" + new string('p', i)));
                    Assert.DoesNotThrow(() => tree.MultiDelete("key2", "items/" + i + "/" + new string('p', i)));
                }

                Assert.True(tree.State.PageCount >= 0);
                Assert.Equal(tree.AllPages().Count, tree.State.PageCount);
            }
        }
    }
}

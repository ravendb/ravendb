// -----------------------------------------------------------------------
//  <copyright file="TreeStateTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using Xunit;
using Voron;
using Voron.Data.BTrees;
using Voron.Global;
using Xunit.Abstractions;

namespace FastTests.Voron.Trees
{
    public class TreeStateTests : StorageTest
    {
        public TreeStateTests(ITestOutputHelper output) : base(output)
        {
        }

        protected override void Configure(StorageEnvironmentOptions options)
        {
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

                ref readonly var treeState = ref tree.State.Header;

                Assert.True(treeState.PageCount > 0);
                Assert.Equal(treeState.PageCount, treeState.BranchPages + treeState.LeafPages + treeState.OverflowPages);
            }
        }

        [Fact]
        public void HasReducedNumberOfPagesAfterRemovingHalfOfEntries()
        {
            int numberOfRegularItems = 0;
            int numberOfOverflowItems = 0;
            var testKey = "test" + new string('-', 128);
            var smallValue = new byte[256];
            var largeValue = new byte[Constants.Storage.PageSize * 2];
            var overflowKey = "overflow" + new string('-', 128);

            TreeMutableState old;
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                var size = 0;

                while (size < 32 * Constants.Storage.PageSize)
                {
                    var key = testKey + numberOfRegularItems++;
                    tree.Add(key, smallValue);
                    size += Tree.CalcSizeOfEmbeddedEntry(key.Length, smallValue.Length);
                }

                size = 0;

                while (size < 6 * Constants.Storage.PageSize)
                {
                    var key = overflowKey + numberOfOverflowItems++;
                    tree.Add(key, largeValue);

                    size += Tree.CalcSizeOfEmbeddedEntry(key.Length, largeValue.Length);
                }

                tx.Commit();

                old = tree.State.Clone();
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

                
                ref readonly var state = ref tree.State.Header;
                ref readonly var oldState = ref old.Header;

                Assert.True(oldState.PageCount > state.PageCount);
                Assert.True(oldState.LeafPages > state.LeafPages);
                Assert.True(oldState.BranchPages >= state.BranchPages);
                Assert.True(oldState.OverflowPages > state.OverflowPages);
                Assert.True(oldState.Depth >= state.Depth);
            }
        }

        [Fact]
        public void HasReducedTreeDepthValueAfterRemovingEntries()
        {
            int numberOfItems = 0;
            TreeMutableState old;
            var key = "test" + new string('-', 256);
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("test");

                int size = 0;
                var value = new byte[256];

                while (size < 128*Constants.Storage.PageSize)
                {
                    numberOfItems++;
                    var s = key + numberOfItems;
                    tree.Add(s, value);
                    size += Tree.CalcSizeOfEmbeddedEntry(s.Length, 256);
                }
                old = tree.State.Clone();

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.ReadTree("test");

                for (int i = 0; i < numberOfItems * 0.75; i++)
                {
                    tree.Delete(key + i);
                }
                Assert.True(old.Header.Depth >= tree.State.Header.Depth);

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.ReadTree("test");

                for (int i = 0; i < numberOfItems; i++)
                {
                    tree.Delete(key + i);
                }

                Assert.Equal(1, tree.State.Header.Depth);
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

                Assert.True(tree.State.Header.PageCount >= 0);
                Assert.Equal(tree.AllPages().Count, tree.State.Header.PageCount);
            }
        }
    }
}

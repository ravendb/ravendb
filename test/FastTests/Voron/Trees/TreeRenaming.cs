// -----------------------------------------------------------------------
//  <copyright file="TreeRenaming.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using Xunit;
using Voron.Global;
using Xunit.Abstractions;

namespace FastTests.Voron.Trees
{
    public class TreeRenaming : StorageTest
    {
        public TreeRenaming(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanRenameTree()
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("tree");

                tree.Add("items/1", new byte[] { 1, 2, 3 });
                tree.Add("items/2", new byte[] { 1, 2, 3 });
                tree.Add("items/3", new byte[] { 1, 2, 3 });

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                tx.RenameTree( "tree", "renamed_tree");

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadTree("renamed_tree");

                var readResult = tree.Read("items/1");
                Assert.NotNull(readResult);

                readResult = tree.Read("items/2");
                Assert.NotNull(readResult);

                readResult = tree.Read("items/3");
                Assert.NotNull(readResult);
            }
        }

        [Fact]
        public void ShouldNotAllowToRenameTreeIfTreeAlreadyExists()
        {
            using (var tx = Env.WriteTransaction())
            {
                tx.CreateTree("tree_1");
                tx.CreateTree("tree_2");

                var ae = Assert.Throws<ArgumentException>(() => tx.RenameTree("tree_1", "tree_2"));

                Assert.StartsWith("Cannot rename a tree with the name of an existing tree: tree_2", ae.Message);
            }
        }

        [Fact]
        public void ShouldThrowIfTreeDoesNotExist()
        {
            using (var tx = Env.WriteTransaction())
            {
                var ae = Assert.Throws<ArgumentException>(() => tx.RenameTree( "tree_1", "tree_2"));

                Assert.StartsWith("Tree tree_1 does not exists", ae.Message);
            }
        }

        [Fact]
        public void MustNotRenameToRootAndFreeSpaceRootTrees()
        {
            using (var tx = Env.WriteTransaction())
            {
                var ex = Assert.Throws<InvalidOperationException>(() => tx.RenameTree("tree_1", Constants.RootTreeName));
                Assert.Equal("Cannot create a tree with reserved name: " + Constants.RootTreeName, ex.Message);
            }
        }

        [Fact]
        public void ShouldPreventFromRenamingTreeInReadTransaction()
        {
            using (var tx = Env.ReadTransaction())
            {
                var ae = Assert.Throws<InvalidOperationException>(() => tx.RenameTree( "tree_1", "tree_2"));

                Assert.Equal("Cannot rename a new tree with a read only transaction", ae.Message);
            }
        }
    }
}

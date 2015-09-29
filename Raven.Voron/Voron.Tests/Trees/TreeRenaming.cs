// -----------------------------------------------------------------------
//  <copyright file="TreeRenaming.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Voron.Impl;
using Xunit;

namespace Voron.Tests.Trees
{
	public class TreeRenaming : StorageTest
	{
		[Fact]
		public void CanRenameTree()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var tree = Env.CreateTree(tx, "tree");

				tree.Add("items/1", new byte[] { 1, 2, 3 });
				tree.Add("items/2", new byte[] { 1, 2, 3 });
				tree.Add("items/3", new byte[] { 1, 2, 3 });

				tx.Commit();
			}

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				Env.RenameTree(tx, "tree", "renamed_tree");

				tx.Commit();
			}

			using (var tx = Env.NewTransaction(TransactionFlags.Read))
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
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				Env.CreateTree(tx, "tree_1");
				Env.CreateTree(tx, "tree_2");

				var ae = Assert.Throws<ArgumentException>(() => Env.RenameTree(tx, "tree_1", "tree_2"));

				Assert.Equal("Cannot rename a tree with the name of an existing tree: tree_2", ae.Message);
			}
		}

		[Fact]
		public void ShouldThrowIfTreeDoesNotExist()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var ae = Assert.Throws<ArgumentException>(() => Env.RenameTree(tx, "tree_1", "tree_2"));

				Assert.Equal("Tree tree_1 does not exists", ae.Message);
			}
		}

		[Fact]
		public void MustNotRenameToRootAndFreeSpaceRootTrees()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var ex = Assert.Throws<InvalidOperationException>(() => Env.RenameTree(tx, "tree_1", Constants.RootTreeName));
				Assert.Equal("Cannot create a tree with reserved name: " + Constants.RootTreeName, ex.Message);

				ex = Assert.Throws<InvalidOperationException>(() => Env.RenameTree(tx, "tree_1", Constants.FreeSpaceTreeName));
				Assert.Equal("Cannot create a tree with reserved name: " + Constants.FreeSpaceTreeName, ex.Message);
			}
		}

		[Fact]
		public void ShouldPreventFromRenamingTreeInReadTransaction()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.Read))
			{
				var ae = Assert.Throws<ArgumentException>(() => Env.RenameTree(tx, "tree_1", "tree_2"));

				Assert.Equal("Cannot rename a new tree with a read only transaction", ae.Message);
			}
		}
	}
}
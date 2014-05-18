using System;
using Voron.Impl;
using Voron.Trees;

// ReSharper disable once CheckNamespace
namespace Voron
{
	public static class TreeExtensions
	{
		public static Tree GetTree(this StorageEnvironmentState state, Transaction tx, string treeName)
		{			
			if (String.IsNullOrEmpty(treeName))
				throw new InvalidOperationException("Cannot fetch tree with empty name");

            if (treeName.Equals(Constants.RootTreeName, StringComparison.InvariantCultureIgnoreCase))
                return state.Root;

            if (treeName.Equals(Constants.FreeSpaceTreeName, StringComparison.InvariantCultureIgnoreCase))
                return state.FreeSpaceRoot;


		    Tree tree = tx.ReadTree(treeName);

			if (tree != null)
				return tree;

			if (tx.Flags == TransactionFlags.ReadWrite)
				return tx.Environment.CreateTree(tx, treeName);

			throw new InvalidOperationException("No such tree: " + treeName);			
		}
	}
}

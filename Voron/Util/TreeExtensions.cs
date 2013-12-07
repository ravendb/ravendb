using System;
using Voron.Impl;
using Voron.Trees;

// ReSharper disable once CheckNamespace
namespace Voron
{
	public static class TreeExtensions
	{
		public static Tree GetTree(this StorageEnvironmentState state,string treeName,Transaction tx)
		{			
			if (String.IsNullOrEmpty(treeName))
				throw new InvalidOperationException("Cannot fetch tree with empty name");			

			Tree tree;

			if (state.Trees.TryGetValue(treeName, out tree))
				return tree;

			if (treeName.Equals(Constants.RootTreeName, StringComparison.InvariantCultureIgnoreCase))
				return state.Root;

			if (treeName.Equals(Constants.FreeSpaceTreeName, StringComparison.InvariantCultureIgnoreCase))
				return state.FreeSpaceRoot;

			throw new InvalidOperationException("No such tree: " + treeName);			
		}
	}
}

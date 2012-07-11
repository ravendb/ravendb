//-----------------------------------------------------------------------
// <copyright file="PersistentDictionaryState.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using Raven.Json.Linq;
using Raven.Munin.Tree;

namespace Raven.Munin
{
	public class PersistentDictionaryState
	{
		public IBinarySearchTree<RavenJToken, PositionInFile> KeyToFilePositionInFiles { get; set; }

		public List<IBinarySearchTree<IComparable, IBinarySearchTree<RavenJToken, RavenJToken>>> SecondaryIndicesState { get; set; }

		public IComparerAndEquality<RavenJToken> Comparer { get; set; }

		public PersistentDictionaryState(IComparerAndEquality<RavenJToken> comparer)
		{
			Comparer = comparer;
			SecondaryIndicesState = new List<IBinarySearchTree<IComparable, IBinarySearchTree<RavenJToken, RavenJToken>>>();
			KeyToFilePositionInFiles = new EmptyAVLTree<RavenJToken, PositionInFile>(Comparer, token => token.CloneToken(), file => new PositionInFile
			{
				Key = file.Key.CloneToken(),
				Position = file.Position,
				Size = file.Size
			});
		}
	}
}
//-----------------------------------------------------------------------
// <copyright file="SecondaryIndex.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Raven.Json.Linq;
using Raven.Munin.Tree;

namespace Raven.Munin
{
	public class SecondaryIndex
	{
		private readonly string indexDef;
		private IPersistentSource persistentSource;
		private readonly Func<RavenJToken, IComparable> transform;

		public SecondaryIndex(Func<RavenJToken, IComparable> transform, string indexDef)
		{
			this.transform = transform;
			this.indexDef = indexDef;
			IndexId = -1;
		}

		private IBinarySearchTree<IComparable, IBinarySearchTree<RavenJToken, RavenJToken>> Index
		{
			get { return persistentSource.DictionariesStates[DictionaryId].SecondaryIndicesState[IndexId]; }
			set { persistentSource.DictionariesStates[DictionaryId].SecondaryIndicesState[IndexId] = value; }
		}

		public long Count
		{
			get { return Index.Count; }
		}

		public int DictionaryId { get; set; }

		public int IndexId { get; set; }

		public string Name { get; set; }

		public override string ToString()
		{
			if (IndexId == -1)
				return Name + ": " + indexDef;
			return Name + ": " + indexDef + " (" + Index.Count + ")";
		}
		// This is called only from inside persistenceStore.Write

		public void Add(RavenJToken key)
		{
			IComparable actualKey = transform(key);
			Index = Index.AddOrUpdate(actualKey,
				new EmptyAVLTree<RavenJToken, RavenJToken>(RavenJTokenComparer.Instance, token => token.CloneToken(), token => token.CloneToken()).Add(key, key),
				(comparable, tree) => tree.Add(key, key));
		}

		// This is called only from inside persistenceStore.Write
		public void Remove(RavenJToken key)
		{
			IComparable actualKey = transform(key);
			var result = Index.Search(actualKey);
			if (result.IsEmpty)
			{
				return;
			}
			bool removed;
			RavenJToken _;
			var removedResult = result.Value.TryRemove(key, out removed, out _);
			if (removedResult.IsEmpty)
			{
				IBinarySearchTree<RavenJToken, RavenJToken> ignored;
				Index = Index.TryRemove(actualKey, out removed, out ignored);
			}
			else
			{
				Index = Index.AddOrUpdate(actualKey, removedResult, (comparable, tree) => removedResult);
			}
		}


		public IEnumerable<RavenJToken> SkipFromEnd(int start)
		{
			return persistentSource.Read(() => Index.ValuesInReverseOrder.Skip(start).Select(item => item.Key));
		}

		public IEnumerable<RavenJToken> SkipAfter(RavenJToken key)
		{
			return
				persistentSource.Read(
					() =>
					Index.GreaterThan(transform(key)).SelectMany(binarySearchTree => binarySearchTree.ValuesInOrder));
		}

		public IEnumerable<RavenJToken> SkipBefore(RavenJToken key)
		{
			return
				persistentSource.Read(
					() =>
					Index.LessThan(transform(key)).Select(binarySearchTree => binarySearchTree.Value));
		}


		public RavenJToken GreatestEqual(RavenJObject key, Predicate<RavenJToken> predicate)
		{
			return persistentSource.Read(
				() =>
				{
					var nearest = Index.LocateNearest(transform(key), tree => predicate(tree.Value));
					if (nearest.IsEmpty)
						return null;

					var binarySearchTree = nearest.ValuesInOrder
						.SkipWhile(x => predicate(x.Value) == false)
						.TakeWhile(x => predicate(x.Value))
						.LastOrDefault();
					if (binarySearchTree == null)
						return null;
					return binarySearchTree.Value;

				});
		}

		public IEnumerable<RavenJToken> SkipTo(RavenJToken key)
		{
			return
				persistentSource.Read(
					() =>
					Index.GreaterThanOrEqual(transform(key)).SelectMany(
						binarySearchTree => binarySearchTree.ValuesInOrder));
		}

		public RavenJToken LastOrDefault()
		{
			return persistentSource.Read(()=>
			{
				if (Index.RightMost.IsEmpty)
					return null;
				var binarySearchTree = Index.RightMost.Value.RightMost;
				if (binarySearchTree.IsEmpty)
					return null;
				return binarySearchTree.Value;
			});
		}

		public RavenJToken FirstOrDefault()
		{
			return persistentSource.Read(() =>
			{
				if (Index.LeftMost.IsEmpty)
					return null;
				var binarySearchTree = Index.LeftMost.Value.LeftMost;
				if (binarySearchTree.IsEmpty)
					return null;
				return binarySearchTree.Value;
			});
		}

		public void Initialize(IPersistentSource thePersistentSource, int dictionaryId, int indexId)
		{
			DictionaryId = dictionaryId;
			IndexId = indexId;
			persistentSource = thePersistentSource;
		}

	}
}
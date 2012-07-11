//-----------------------------------------------------------------------
// <copyright file="EmptyAVLTree.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using Raven.Json.Linq;

namespace Raven.Munin.Tree
{
	internal sealed class EmptyAVLTree<TKey, TValue> : IBinarySearchTree<TKey, TValue>
	{
		private readonly IComparer<TKey> comparer;
		private readonly Func<TKey, TKey> deepCopyKey;
		private readonly Func<TValue, TValue> deepCopyValue;


		public EmptyAVLTree(IComparer<TKey> comparer, Func<TKey, TKey> deepCopyKey, Func<TValue, TValue> deepCopyValue)
		{
			this.comparer = comparer;
			this.deepCopyValue = deepCopyValue;
			this.deepCopyKey = deepCopyKey;
		}

		// IBinaryTree

		#region IBinarySearchTree<K,TValue> Members

		public IComparer<TKey> Comparer
		{
			get { return comparer; }
		}

		public int Count
		{
			get { return 0; }
		}

		public bool IsEmpty
		{
			get { return true; }
		}

		public TValue Value
		{
			get { throw new Exception("empty tree"); }
		}

		public RavenJObject ToJObject()
		{
			return new RavenJObject();
		}

		public IBinarySearchTree<TKey, TValue> LeftMost
		{
			get { return this; }
		}

		public IBinarySearchTree<TKey, TValue> RightMost
		{
			get { return this; }
		}

		// IBinarySearchTree
		public IBinarySearchTree<TKey, TValue> Left
		{
			get { throw new Exception("empty tree"); }
		}

		public IBinarySearchTree<TKey, TValue> Right
		{
			get { throw new Exception("empty tree"); }
		}

		public IBinarySearchTree<TKey, TValue> Search(TKey key)
		{
			return this;
		}

		public IEnumerable<TValue> GreaterThan(TKey gtKey)
		{
			yield break;
		}

		public IEnumerable<TValue> LessThan(TKey ltKey)
		{
			yield break;
		}

		public IEnumerable<TValue> LessThanOrEqual(TKey ltKey)
		{
			yield break;
		}

		public IEnumerable<TValue> GreaterThanOrEqual(TKey gteKey)
		{
			yield break;
		}

		public TKey Key
		{
			get { throw new Exception("empty tree"); }
		}

		public IBinarySearchTree<TKey, TValue> Add(TKey key, TValue value)
		{
			return new AVLTree<TKey, TValue>(comparer,deepCopyKey, deepCopyValue, key, value, this, this);
		}

		public IBinarySearchTree<TKey, TValue> AddOrUpdate(TKey key, TValue value, Func<TKey, TValue, TValue> updateValueFactory)
		{
			// we don't udpate, so we don't care about the update value factory
			return new AVLTree<TKey, TValue>(comparer, deepCopyKey, deepCopyValue, key, value, this, this);
		}

		public IBinarySearchTree<TKey, TValue> TryRemove(TKey key, out bool removed, out TValue value)
		{
			removed = false;
			value = default(TValue);
			return this;
		}

		// IMap
		public bool Contains(TKey key)
		{
			return false;
		}

		public IBinarySearchTree<TKey, TValue> LocateNearest(TKey key, Predicate<TValue> isMatch)
		{
			return this;
		}

		public bool TryGetValue(TKey key, out TValue value)
		{
			value = default(TValue);
			return false;
		}

		public IEnumerable<TKey> KeysInOrder
		{
			get { yield break; }
		}

		public IEnumerable<TKey> KeysInReverseOrder
		{
			get { yield break; }
		}


		public IEnumerable<TValue> ValuesInOrder
		{
			get { yield break; }
		}

		public IEnumerable<TValue> ValuesInReverseOrder
		{
			get { yield break; }
		}

		public IEnumerable<KeyValuePair<TKey, TValue>> Pairs
		{
			get { yield break; }
		}

		#endregion
	}
}
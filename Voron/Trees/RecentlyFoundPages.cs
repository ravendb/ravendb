// -----------------------------------------------------------------------
//  <copyright file="BinaryTree.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using Voron.Impl;

namespace Voron.Trees
{
	public unsafe class RecentlyFoundPages
	{
		public class FoundPage
		{
			public long Number;
			public Slice FirstKey;
			public Slice LastKey;
			public List<long> CursorPath;
		}

		public class Node
		{
			public Slice Key;
			public FoundPage Value;
			public Node Left, Right;

			public Node(Slice key, FoundPage value)
			{
				Key = key;
				Value = value;
			}
		}

		private const int CacheLimit = 128;

		private Node _root;
		private readonly LinkedList<Node> _lru = new LinkedList<Node>();
		private int _count = 0;

		public int Count
		{
			get { return _count; }
		}

		public void Add(FoundPage page)
		{
			var key = page.FirstKey;

			int cmp;

			Node current = _root, parent = null;
			
			while (current != null)
			{
				cmp = Compare(current.Key, key);

				if (cmp == 0)
				{
					// the key is already in tree - update the value and LRU list
					current.Value = page;

					_lru.Remove(current);
					_lru.AddFirst(current);

					return;
				}

				if (cmp > 0)
				{
					parent = current;
					current = current.Left;
				}
				else if (cmp < 0)
				{
					parent = current;
					current = current.Right;
				}
			}

			var newNode = new Node(key, page);

			_count++;
			if (parent == null)
				_root = newNode;
			else
			{
				cmp = Compare(parent.Key, key);

				if (cmp > 0)
					parent.Left = newNode;
				else
					parent.Right = newNode;
			}

			_lru.AddLast(newNode);

			if (_count > CacheLimit)
			{
				Debug.Assert(_count == _lru.Count);

				Delete(_lru.First.Value.Key);
				_lru.RemoveFirst();
			}
		}

		private int Compare(Slice x, Slice y)
		{
			int cmp;

			if (x.Options == SliceOptions.BeforeAllKeys)
			{
				if (y.Options == SliceOptions.BeforeAllKeys)
					cmp = 0;
				else
					cmp = -1;
			}
			else if (y.Options == SliceOptions.BeforeAllKeys)
			{
				Debug.Assert(x.Options == SliceOptions.Key);
				cmp = 1;
			}
			else if (x.Options == SliceOptions.AfterAllKeys)
				throw new InvalidOperationException("Should never happen");
			else
			{
				cmp = x.Compare(y, NativeMethods.memcmp);
			}

			return cmp;
		}

		public FoundPage Find(Slice key)
		{
			if (_root == null)
				return null;

			var current = _root;
			int cmp = 0;

			while (true)
			{
				cmp = Compare(key, current.Key);
				if (cmp < 0)
				{
					if (current.Left == null)
						break;

					current = current.Left;
				}
				else if (cmp > 0)
				{
					if (current.Right == null)
						break;
					current = current.Right;
				}
				else
					return current.Value;
			}

			var first = current.Value.FirstKey;
			var last = current.Value.LastKey;

			if (key.Options == SliceOptions.BeforeAllKeys && first.Options != SliceOptions.BeforeAllKeys)
				return null;

			if (key.Options == SliceOptions.AfterAllKeys && last.Options != SliceOptions.AfterAllKeys)
				return null;

			Debug.Assert(key.Options == SliceOptions.Key);

			if (first.Options != SliceOptions.BeforeAllKeys && key.Compare(first, NativeMethods.memcmp) < 0 ||
			    last.Options != SliceOptions.AfterAllKeys && key.Compare(last, NativeMethods.memcmp) > 0)
				return null;

			_lru.Remove(current);
			_lru.AddLast(current);

			return current.Value;
		}

		private void Delete(Slice key)
		{
			if (_root == null)
				return;       // no items to remove

			// Now, try to find data in the tree
			Node current = _root, parent = null;
			int result = Compare(current.Key, key);
			while (result != 0)
			{
				if (result > 0)
				{
					// current.Value > data, if data exists it's in the left subtree
					parent = current;
					current = current.Left;
				}
				else if (result < 0)
				{
					// current.Value < data, if data exists it's in the right subtree
					parent = current;
					current = current.Right;
				}

				Debug.Assert(current != null);// if current == null, then we didn't find the item to remove - should never happen
				
			result = Compare(current.Key, key);
			}

			// At this point, we've found the node to remove
			_count--;

			// We now need to "rethread" the tree
			// CASE 1: If current has no right child, then current's left child becomes
			//         the node pointed to by the parent
			if (current.Right == null)
			{
				if (parent == null)
					_root = current.Left;
				else
				{
					result = Compare(parent.Key, current.Key);
					if (result > 0)
						// parent.Value > current.Value, so make current's left child a left child of parent
						parent.Left = current.Left;
					else if (result < 0)
						// parent.Value < current.Value, so make current's left child a right child of parent
						parent.Right = current.Left;
				}
			}
			// CASE 2: If current's right child has no left child, then current's right child
			//         replaces current in the tree
			else if (current.Right.Left == null)
			{
				current.Right.Left = current.Left;

				if (parent == null)
					_root = current.Right;
				else
				{
					result = Compare(parent.Key, current.Key);
					if (result > 0)
						// parent.Value > current.Value, so make current's right child a left child of parent
						parent.Left = current.Right;
					else if (result < 0)
						// parent.Value < current.Value, so make current's right child a right child of parent
						parent.Right = current.Right;
				}
			}
			// CASE 3: If current's right child has a left child, replace current with current's
			//          right child's left-most descendent
			else
			{
				// We first need to find the right node's left-most child
				Node leftmost = current.Right.Left, lmParent = current.Right;
				while (leftmost.Left != null)
				{
					lmParent = leftmost;
					leftmost = leftmost.Left;
				}

				// the parent's left subtree becomes the leftmost's right subtree
				lmParent.Left = leftmost.Right;

				// assign leftmost's left and right to current's left and right children
				leftmost.Left = current.Left;
				leftmost.Right = current.Right;

				if (parent == null)
					_root = leftmost;
				else
				{
					result = Compare(parent.Key, current.Key);
					if (result > 0)
						// parent.Value > current.Value, so make leftmost a left child of parent
						parent.Left = leftmost;
					else if (result < 0)
						// parent.Value < current.Value, so make leftmost a right child of parent
						parent.Right = leftmost;
				}
			}
		}

		public void Clear()
		{
			_root = null;
			_count = 0;
			_lru.Clear();
		}
	}
}

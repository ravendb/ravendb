// -----------------------------------------------------------------------
//  <copyright file="BinaryTree.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using Voron.Impl;

namespace Voron.Util
{
	public class RecentlyFoundPages
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

		public void Add(FoundPage page)
		{
			var key = page.FirstKey;

			Node modifiedNode;
			var itemsCount = _count;
			_root = AddNodeInternal(_root, key, page, out modifiedNode);

			if (_count > itemsCount) // new page added
			{
				_lru.AddLast(modifiedNode);

				Debug.Assert(_lru.Count == _count);

				if (_count > CacheLimit)
				{
					Delete(_lru.First.Value.Key);
					_lru.RemoveFirst();
				}
			}
			else // page updated
			{
				_lru.Remove(modifiedNode);
				_lru.AddFirst(modifiedNode);
			}
		}

		private unsafe Node AddNodeInternal(Node node, Slice key, FoundPage val, out Node modifiedNode)
		{
			if (node == null)
			{
				_count++;
				return modifiedNode = new Node(key, val);
			}

			int cmp = 0;

			if (key.Options == SliceOptions.BeforeAllKeys)
			{	
				if (node.Key.Options == SliceOptions.BeforeAllKeys)
					cmp = 0;
				else
					cmp = -1;
			}
			else if(key.Options == SliceOptions.AfterAllKeys)
				throw new InvalidOperationException("Should never happen");
			else
			{
				cmp = key.Compare(node.Key, NativeMethods.memcmp);
			}

			if (cmp < 0)
				node.Left = AddNodeInternal(node.Left, key, val, out modifiedNode);
			else if (cmp > 0)
				node.Right = AddNodeInternal(node.Right, key, val, out modifiedNode);
			else
			{
				node.Value = val;
				modifiedNode = node;
			}
			
			return node;
		}

		public unsafe FoundPage Find(Slice key)
		{
			if (_root == null)
				return null;

			Node currentNode = _root;
			int cmp = 0;

			while (true)
			{
				if (key.Options != SliceOptions.Key)
				{
					throw new InvalidOperationException("AAAREK");
				}
				else
				{
					cmp = key.Compare(currentNode.Key, NativeMethods.memcmp);
				}

				if (cmp < 0)
				{
					if (currentNode.Left == null)
						break;

					currentNode = currentNode.Left;
				}
				else if (cmp > 0)
				{
					if (currentNode.Right == null)
						break;
					currentNode = currentNode.Right;
				}
				else
					return currentNode.Value;
			}

			var first = currentNode.Value.FirstKey;
			var last = currentNode.Value.LastKey;

			if (key.Options == SliceOptions.BeforeAllKeys && first.Options != SliceOptions.BeforeAllKeys)
				return null;

			if (key.Options == SliceOptions.AfterAllKeys && last.Options != SliceOptions.AfterAllKeys)
				return null;

			Debug.Assert(key.Options == SliceOptions.Key);

			if (first.Options != SliceOptions.BeforeAllKeys && key.Compare(first, NativeMethods.memcmp) < 0 ||
			    last.Options != SliceOptions.AfterAllKeys && key.Compare(last, NativeMethods.memcmp) > 0)
				return null;

			_lru.Remove(currentNode);
			_lru.AddLast(currentNode);

			return currentNode.Value;
		}

		private void Delete(Slice key)
		{
			if (_root != null)
			{
				_root = Delete(_root, key);
				_count--;
			}
		}

		private unsafe Node Delete(Node node, Slice key)
		{
			if (node == null)
				return null;

			int cmp = 0;

			if (key.Options == SliceOptions.BeforeAllKeys)
			{
				if (node.Key.Options == SliceOptions.BeforeAllKeys)
					cmp = 0;
				else
					cmp = -1;
			}
			else if (key.Options == SliceOptions.AfterAllKeys)
				throw new InvalidOperationException("Should never happen");
			else
				cmp = key.Compare(node.Key, NativeMethods.memcmp);

			if (cmp < 0)
				node.Left = Delete(node.Left, key);
			else if (cmp > 0)
				node.Right = Delete(node.Right, key);
			else
			{
				bool isRoot = _root == node;

				if (node.Left == null)
					return node.Right;

				if (node.Right == null)
					return node.Left;

				Node t = node;
				node = FindMin(t.Right);
				node.Right = DeleteMin(t.Right);
				node.Left = t.Left;

				if (isRoot)
					_root = node;
			}

			return node;
		}

		private Node FindMin(Node node)
		{
			Debug.Assert(node != null);

			if (node.Left == null)
				return node;

			return FindMin(node.Left);
		}

		private Node DeleteMin(Node node)
		{
			if (node.Left == null)
				return node.Right;

			node.Left = DeleteMin(node.Left);
			return node;
		}
	}
}

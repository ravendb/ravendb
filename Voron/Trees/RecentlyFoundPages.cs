// -----------------------------------------------------------------------
//  <copyright file="BinaryTree.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using Voron.Impl;
using Voron.Util;

namespace Voron.Trees
{
	public unsafe class RecentlyFoundPages
	{
		private class FoundPagesComparer : IComparer<Slice>
		{
			public int Compare(Slice x, Slice y)
			{
				int cmp;

				if (x.Options == SliceOptions.AfterAllKeys)
				{
					if (y.Options == SliceOptions.AfterAllKeys)
						cmp = 0;
					else
						cmp = 1;
				}
				else if (y.Options == SliceOptions.AfterAllKeys)
				{
					Debug.Assert(x.Options == SliceOptions.Key);
					cmp = -1;
				}
				else if (y.Options == SliceOptions.BeforeAllKeys)
				{
					if (x.Options == SliceOptions.BeforeAllKeys)
						cmp = 0;
					else
						cmp = 1;
				}
				else
				{
					cmp = x.Compare(y, NativeMethods.memcmp);
				}

				return cmp;
			}
		}

		public class FoundPage
		{
			public long Number;
			public Slice FirstKey;
			public Slice LastKey;
			public List<long> CursorPath;
		}

		private const int CacheLimit = 512;

		public int Count
		{
			get { return _list.Count; }
		}

		private SkipList<Slice, FoundPage> _list = new SkipList<Slice, FoundPage>(_foundPagesComparer);
		private static FoundPagesComparer _foundPagesComparer = new FoundPagesComparer();
		private LinkedList<SkipList<Slice, FoundPage>.Node> _lru = new LinkedList<SkipList<Slice, FoundPage>.Node>(); 

		public void Add(FoundPage page)
		{
			var key = page.LastKey;

			SkipList<Slice, FoundPage>.Node node;

			if (_list.Insert(key, page, out node)) // new item added
			{
				_lru.AddLast(node);

				if (_list.Count > CacheLimit)
				{
					Debug.Assert(_list.Count == _lru.Count);

					if (_list.Remove(_lru.First.Value.Key) == false)
						throw new InvalidOperationException("Should never happen");

					_lru.RemoveFirst();
				}
			}
			else // update
			{
				if (_lru.First.Value != node)
				{
					_lru.Remove(node);
					_lru.AddFirst(node);
				}
			}
		}

		public FoundPage Find(Slice key)
		{
			if (_list.Count == 0)
				return null;

			var current = _list.FindGreaterOrEqual(key, null);

			if (current == null)
				return null;

			var first = current.Val.FirstKey;
			var last = current.Val.LastKey;

			if (key.Options == SliceOptions.BeforeAllKeys && first.Options != SliceOptions.BeforeAllKeys)
				return null;

			if (key.Options == SliceOptions.AfterAllKeys && last.Options != SliceOptions.AfterAllKeys)
				return null;

			Debug.Assert(key.Options == SliceOptions.Key);

			if (first.Options != SliceOptions.BeforeAllKeys && key.Compare(first, NativeMethods.memcmp) < 0 ||
			    last.Options != SliceOptions.AfterAllKeys && key.Compare(last, NativeMethods.memcmp) > 0)
				return null;

			if (_lru.First.Value != current)
			{
				_lru.Remove(current);
				_lru.AddFirst(current);
			}

			return current.Val;
		}

		public void Clear()
		{
			_list = new SkipList<Slice, FoundPage>(_foundPagesComparer);
			_lru.Clear();
		}
	}
}

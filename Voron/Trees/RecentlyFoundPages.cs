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
		private class FoundPagesComparer : IComparer<FoundPage>
		{
			public int Compare(FoundPage x, FoundPage y)
			{
				if (x.Number == y.Number)
					return 0;

				int cmp;

				if (x.LastKey.Options == SliceOptions.AfterAllKeys)
				{
					if (y.LastKey.Options == SliceOptions.AfterAllKeys)
						cmp = 0;
					else
						cmp = 1;
				}
				else if (y.LastKey.Options == SliceOptions.AfterAllKeys)
				{
					Debug.Assert(x.LastKey.Options == SliceOptions.Key);
					cmp = -1;
				}
				else if (y.LastKey.Options == SliceOptions.BeforeAllKeys)
				{
					if (x.LastKey.Options == SliceOptions.BeforeAllKeys)
						cmp = 0;
					else
						cmp = 1;
				}
				else
				{
					cmp = x.LastKey.Compare(y.LastKey, NativeMethods.memcmp);
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

		private SkipList<FoundPage> _list = new SkipList<FoundPage>(_foundPagesComparer);
		private static FoundPagesComparer _foundPagesComparer = new FoundPagesComparer();
		private LinkedList<SkipList<FoundPage>.Node> _lru = new LinkedList<SkipList<FoundPage>.Node>(); 

		public void Add(FoundPage page)
		{
			SkipList<FoundPage>.Node node;

			if (_list.Insert(page, out node)) // new item added
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

		private readonly FoundPage _pageForSearchingByKey = new FoundPage {Number = long.MinValue};

		public FoundPage Find(Slice key)
		{
			if (_list.Count == 0)
				return null;

			_pageForSearchingByKey.LastKey = key;

			var current = _list.FindGreaterOrEqual(_pageForSearchingByKey, null);

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
			_list = new SkipList<FoundPage>(_foundPagesComparer);
			_lru.Clear();
		}
	}
}

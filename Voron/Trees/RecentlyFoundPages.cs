// -----------------------------------------------------------------------
//  <copyright file="BinaryTree.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Voron.Impl;
using Voron.Util;

namespace Voron.Trees
{
	public unsafe class RecentlyFoundPages
	{
		private class FoundPagesComparer : IComparer<FoundPage>
		{
            public static readonly FoundPagesComparer Instance = new FoundPagesComparer();
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

		private const int CacheLimit = 128;

		public int Count
		{
			get { return _list.Count; }
		}

		private SkipList<FoundPage> _list = new SkipList<FoundPage>(FoundPagesComparer.Instance);
        private readonly Dictionary<SkipList<FoundPage>.Node, Reference> _lru = new Dictionary<SkipList<FoundPage, FoundPage>.Node, Reference>();

	    private class Reference
	    {
	        public int Usages;
	    }

		public void Add(FoundPage page)
		{
			SkipList<FoundPage>.Node node;

			if (_list.Insert(page, out node)) // new item added
			{
			    _lru[node] = new Reference {Usages = 1};

				if (_list.Count > CacheLimit)
				{
					Debug.Assert(_list.Count == _lru.Count);

                    // if full, we'll remove the oldest 25% (to avoid jittering the cache)
                    // and reduce all usages by half, to make it easy to expire frequently used in the past
                    // but no longer used now items

				    var toRemove = _lru.OrderBy(x => x.Value.Usages).Take(_lru.Count/4).ToArray();
				    foreach (var keyValuePair in toRemove)
				    {
				        var nodeToRemove = keyValuePair.Key;
				        _lru.Remove(nodeToRemove);
				        _list.Remove(nodeToRemove.Key);
				    }

				    foreach (var reference in _lru)
				    {
				        reference.Value.Usages /= 2;
				    }
				}
			}
			else // update
			{
				UpdateLru(node);
			}
		}

	    private void UpdateLru(SkipList<FoundPage, FoundPage>.Node node)
	    {
	        Reference value;
	        if (_lru.TryGetValue(node, out value))
	        {
	            value.Usages++;
	            return;
	        }
	        _lru[node] = new Reference {Usages = 1};
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

            UpdateLru(current);

			return current.Val;
		}

		public void Clear()
		{
			_list = new SkipList<FoundPage>(FoundPagesComparer.Instance);
			_lru.Clear();
		}
	}
}

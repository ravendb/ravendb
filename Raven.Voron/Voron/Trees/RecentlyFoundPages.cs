// -----------------------------------------------------------------------
//  <copyright file="BinaryTree.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Diagnostics;
using System.Runtime.InteropServices;
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
			public readonly long[] CursorPath;

		    public FoundPage(int size)
		    {
		        CursorPath = new long[size];
		    }
		}

	    private readonly FoundPage[] _cache;

	    private readonly int _cacheSize;

	    public RecentlyFoundPages(int cacheSize)
	    {
            _cache = new FoundPage[cacheSize];
	        _cacheSize = cacheSize;
	    }

	    public void Add(FoundPage page)
		{
#if DEBUG
	        if ((page.FirstKey.Options==SliceOptions.BeforeAllKeys) && (page.LastKey.Options == SliceOptions.AfterAllKeys))
	        {
	            Debug.Assert(page.CursorPath.Length == 1);
	        }
#endif

            for (int i = 0; i < _cacheSize; i++)
		    {
                if (_cache[i] == null || _cache[i].Number == page.Number)
                {
                    _cache[0] = page;
                    return;
                }
		    }

	        for (int i = 1; i < _cacheSize; i++)
	        {
	            _cache[i] = _cache[i - 1];
	        }
		    _cache[0] = page;
		}

		public FoundPage Find(Slice key)
		{
            for (int i = 0; i < _cache.Length; i++)
		    {
		        var page = _cache[i];
                if(page == null)
                    continue;

                var first = page.FirstKey;
                var last = page.LastKey;

                if (key.Options == SliceOptions.BeforeAllKeys && first.Options != SliceOptions.BeforeAllKeys)
                    return null;

                if (key.Options == SliceOptions.AfterAllKeys && last.Options != SliceOptions.AfterAllKeys)
                    return null;

                Debug.Assert(key.Options == SliceOptions.Key);

                if (first.Options != SliceOptions.BeforeAllKeys && key.Compare(first, NativeMethods.memcmp) < 0 ||
                    last.Options != SliceOptions.AfterAllKeys && key.Compare(last, NativeMethods.memcmp) > 0)
                    return null;

		        return page;
		    }

		    return null;
		}

		public void Clear()
		{
		    Array.Clear(_cache, 0, _cacheSize);
		}
	}
}

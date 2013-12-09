// -----------------------------------------------------------------------
//  <copyright file="BinaryTree.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.CodeDom;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Voron.Impl;
using Voron.Util;

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

	    private readonly FoundPage[] _cache = new FoundPage[2];

		
		public void Add(FoundPage page)
		{
		    if (_cache[0] == null || _cache[0].Number == page.Number)
		    {
		        _cache[0] = page;
		        return;
		    }
		    if (_cache[1] == null || _cache[1].Number == page.Number)
		    {
		        _cache[1] = page;
		        return;
		    }
		    _cache[1] = _cache[0];
		    _cache[0] = page;
		}

		public FoundPage Find(Slice key)
		{
		    for (int i = 0; i < 2; i++)
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
		    }

		    return null;
		}

		public void Clear()
		{
		    _cache[0] = null;
		    _cache[1] = null;
		}
	}
}

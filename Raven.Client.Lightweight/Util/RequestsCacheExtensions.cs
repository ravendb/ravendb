// -----------------------------------------------------------------------
//  <copyright file="RequestsCacheExtensions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using Raven.Client.Connection;

namespace Raven.Client.Util
{
	internal static class RequestsCacheExtensions
	{
		internal static void ClearOutdatedRequestUrls(this SimpleCache<CachedRequest> cache, DateTimeOffset time)
		{
			foreach (var item in cache.actualCache.Where(x => x.Value.Time > time))
			{
				CachedRequest _;
				cache.actualCache.TryRemove(item.Key, out _);
			}
		}
	}
}
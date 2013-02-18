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
		internal static void ClearItemsForDatabase(this SimpleCache<CachedRequest> cache, string databaseName)
		{
			foreach (var item in cache.actualCache.Where(x => x.Value.Database.Equals(databaseName, StringComparison.Ordinal)))
			{
				CachedRequest _;
				if (cache.actualCache.TryRemove(item.Key, out _))
				{
					cache.lruKeys.Remove(item.Key);
				}
			}
		}
	}
}
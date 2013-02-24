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
		internal static void ForceServerCheckOfCachedItemsForDatabase(this SimpleCache<CachedRequest> cache, string databaseName)
		{
			foreach (var item in cache.actualCache.Where(x => x.Value.Database.Equals(databaseName, StringComparison.Ordinal)))
			{
				item.Value.ForceServerCheck = true;
			}
		}
	}
}
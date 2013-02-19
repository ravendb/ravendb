// -----------------------------------------------------------------------
//  <copyright file="RequestsCacheExtensions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using System.Text.RegularExpressions;
using Raven.Client.Connection;

namespace Raven.Client.Util
{
	internal static class RequestsCacheExtensions
	{
		internal static int ClearItemsForDatabase(this SimpleCache<CachedRequest> cache, string databaseName, string docIdOrIndexName)
		{
			if (docIdOrIndexName == null)
				return 0;

			var numberOfRemovedItems = 0;

			foreach (var item in cache.actualCache.Where(x => x.Value.Database.Equals(databaseName, StringComparison.Ordinal)))
			{
				var cachedUrl = item.Key;
				var cachedData = item.Value.Data;

				var urlMatcher = new Regex(@"(.+)[/=]?" + docIdOrIndexName + @"[/&\?]?(.*)");

				if (urlMatcher.IsMatch(cachedUrl) == false && 
					docIdOrIndexName.Equals(cachedData.Value<string>("IndexName"), StringComparison.Ordinal) == false)
					continue;

				CachedRequest _;
				if (cache.actualCache.TryRemove(item.Key, out _))
				{
					numberOfRemovedItems++;
					cache.lruKeys.Remove(item.Key);
				}
			}

			return numberOfRemovedItems;
		}
	}
}
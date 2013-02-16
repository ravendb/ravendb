// -----------------------------------------------------------------------
//  <copyright file="RequestsCacheExtensions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Connection;

namespace Raven.Client.Util
{
	internal static class RequestsCacheExtensions
	{
		internal static IList<string> GetOutdatedRequestUrls(this SimpleCache<CachedRequest> cache, DateTimeOffset time)
		{
			return cache.actualCache.Where(x => x.Value.Time > time).Select(x => x.Key).ToList();
		}
	}
}
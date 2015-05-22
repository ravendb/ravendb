// -----------------------------------------------------------------------
//  <copyright file="SqlReplicationClassifier.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;

using Raven.Abstractions.Data;
using Raven.Database.Indexing;

namespace Raven.Database.Bundles.SqlReplication
{
	internal static class SqlReplicationClassifier
	{
		private static readonly Dictionary<Etag, List<SqlReplicationConfigWithLastReplicatedEtag>> Empty = new Dictionary<Etag, List<SqlReplicationConfigWithLastReplicatedEtag>>();

		public static Dictionary<Etag, List<SqlReplicationConfigWithLastReplicatedEtag>> GroupConfigs(IList<SqlReplicationConfig> configs, Func<SqlReplicationConfig, Etag> getLastEtagFor)
		{
			if (configs.Count == 0)
				return Empty;

			var configsByEtag = configs
				.Where(x => x.Disabled == false)
				.Select(x => new SqlReplicationConfigWithLastReplicatedEtag(x, getLastEtagFor(x)))
				.GroupBy(x => x.LastReplicatedEtag, DefaultIndexingClassifier.RoughEtagEqualityAndComparison.Instance)
				.OrderByDescending(x => x.Key, DefaultIndexingClassifier.RoughEtagEqualityAndComparison.Instance)
				.ToList();

			if (configsByEtag.Count == 0)
				return Empty;

			return configsByEtag
				.ToDictionary(x => x.Min(y => y.LastReplicatedEtag), x => x.Select(y => y).ToList());
		}
	}
}
// -----------------------------------------------------------------------
//  <copyright file="ReplicationData.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Json.Linq;

namespace Raven.Database.Bundles.Replication.Impl
{
	public class ReplicationData
	{
		public static RavenJArray GetHistory(RavenJObject metadata)
		{
			return (metadata[Constants.RavenReplicationHistory] as RavenJArray) ?? new RavenJArray();
		}

		public static void SetHistory(RavenJObject metadata, RavenJArray history)
		{
			metadata[Constants.RavenReplicationHistory] = history;
		}
	}
}
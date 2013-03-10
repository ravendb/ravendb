// -----------------------------------------------------------------------
//  <copyright file="DatabaseRecord.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;

namespace Raven.ClusterManager.Models
{
	public class DatabaseRecord
	{
		public string Name { get; set; }
		public string ServerId { get; set; }
		public string ServerUrl { get; set; }
		public bool IsReplicationEnabled { get; set; }

		public List<ReplicationDestination> ReplicationDestinations { get; set; }
		public LoadedDatabaseStatistics LoadedDatabaseStatistics { get; set; }
	}
}
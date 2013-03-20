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
		public string Id { get; set; }
		public string Name { get; set; }
		public string ServerId { get; set; }

		private string serverUrl;
		public string ServerUrl
		{
			get { return serverUrl; }
			set { serverUrl = value.TrimEnd('/'); }
		}

		public string DatabaseUrl
		{
			get
			{
				var url = serverUrl;
				if (Name != Constants.SystemDatabase)
				{
					url += "/databases/" + Name;
				}
				return url;
			}
		}

		public bool IsReplicationEnabled { get; set; }

		public List<ReplicationDestination> ReplicationDestinations { get; set; }
		public ReplicationStatistics ReplicationStatistics { get; set; }
		public LoadedDatabaseStatistics LoadedDatabaseStatistics { get; set; }
	}
}
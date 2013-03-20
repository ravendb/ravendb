using System;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;

namespace Raven.ClusterManager.Models
{
	public class ServerRecord
	{
		public string Id { get; set; }
		public string Url { get; set; }
		public string ServerName { get; set; }
		public string ClusterName { get; set; }
		public bool IsOnline { get; set; }

		public DateTimeOffset LastOnlineTime { get; set; }
		public DateTimeOffset LastTriedToConnectAt { get; set; }

		public string[] Databases { get; set; }
		public string[] LoadedDatabases { get; set; }
		public AdminMemoryStatistics MemoryStatistics { get; set; }
		public bool IsUnauthorized { get; set; }

		public string CredentialsId { get; set; }

		public ServerRecord()
		{
			Databases = new string[0];
			LoadedDatabases = new string[0];
		}

		public void NotifyServerIsOnline()
		{
			IsOnline = true;
			LastOnlineTime = DateTimeOffset.UtcNow;
		}
	}

	public enum AuthenticationMode
	{
		None,
		ApiKey,
		User,
	}
}
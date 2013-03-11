using System;
using Raven.Abstractions.Data;

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

		public AuthenticationMode AuthenticationMode { get; set; }
		public string ApiKey { get; set; }
		public string Username { get; set; }
		public string Password { get; set; }
		public string Domain { get; set; }
	}

	public enum AuthenticationMode
	{
		ApiKey,
		User
	}
}
using System;

namespace Raven.ClusterManager.Models
{
	public class ServerRecord
	{
		public string Id { get; set; }
		public string Name { get; set; }
		public string Url { get; set; }
		public string ClusterName { get; set; }
		public bool IsOnline { get; set; }
		public DateTimeOffset LastOnlineTime { get; set; }
	}
}
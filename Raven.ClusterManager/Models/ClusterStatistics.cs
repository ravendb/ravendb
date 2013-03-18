using System.Collections.Generic;

namespace Raven.ClusterManager.Models
{
	public class ClusterStatistics
	{
		public IEnumerable<ServerRecord> Servers { get; set; }
		public IEnumerable<ServerCredentials> Credentials { get; set; }
	}
}
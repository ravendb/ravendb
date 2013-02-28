using System.Collections.Generic;

namespace Raven.ClusterManager.Models
{
	public class ClusterStatistics
	{
		public IEnumerable<ServerRecord> Servers { get; set; }
	}
}
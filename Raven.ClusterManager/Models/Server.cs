namespace Raven.ClusterManager.Models
{
	public class Server
	{
		public string Id { get; set; }
		public string Url { get; set; }
		public string ClusterName { get; set; }

		public string GetClusterName()
		{
			if (string.IsNullOrEmpty(ClusterName))
				return "Cluster1";

			return ClusterName;
		}
	}
}
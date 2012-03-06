namespace Raven.Database.Impl.Clustering
{
	public enum NodeClusterState
	{
		/// <summary>
		/// The Cluster service is not installed on the node.
		/// </summary>
		ClusterStateNotInstalled = 0,

		/// <summary>
		/// The Cluster service is installed on the node but has not yet been configured.
		/// </summary>
		ClusterStateNotConfigured = 1,

		/// <summary>
		/// The Cluster service is installed and configured on the node but is not currently running.
		/// </summary>
		ClusterStateNotRunning = 3,

		/// <summary>
		/// The Cluster service is installed, configured, and running on the node.
		/// </summary>
		ClusterStateRunning = 19,
	}
}
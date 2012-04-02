namespace Raven.Database.Impl.Clustering
{
	public enum ClusterResourceState
	{
		ClusterResourceStateUnknown = -1,
		ClusterResourceInherited = 0,
		ClusterResourceInitializing = 1,
		ClusterResourceOnline = 2,
		ClusterResourceOffline = 3,
		ClusterResourceFailed = 4,
		ClusterResourcePending = 128, 
		ClusterResourceOnlinePending = 129, 
		ClusterResourceOfflinePending = 130 
	}
}
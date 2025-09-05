namespace Raven.Server.Dashboard.Cluster
{
    public enum ClusterDashboardNotificationType
    {
        Unknown,
        ServerTime, // used by studio to sync clocks
        
        CpuUsage,
        MemoryUsage,
        IoStats,
        
        StorageUsage,
        DatabaseStorageUsage,
        
        Traffic,
        DatabaseTraffic,
        
        Indexing,
        DatabaseIndexing,
        
        ClusterOverview,
        DatabaseOverview,
        
        OngoingTasks,

        GcInfo
    }
}
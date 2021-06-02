// -----------------------------------------------------------------------
//  <copyright file="WidgetType.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

namespace Raven.Server.Dashboard.Cluster
{
    public enum ClusterDashboardNotificationType
    {
        Unknown,
        ServerTime, // used by studio to sync clocks
        CpuUsage,
        StorageUsage, 
        MemoryUsage,
        Traffic,
        Indexing,
        
        DatabaseStorageUsage,
        DatabaseTraffic,
        DatabaseIndexing,
        DatabaseOverview
        
        //TODO: ongoing tasks
        //TODO: cluster topology
    }
}

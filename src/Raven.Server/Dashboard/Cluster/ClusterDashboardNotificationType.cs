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
        Debug, // TODO: fake widget used for debugging studio
        CpuUsage,
        License, // used by studio w/o websockets
        StorageUsage, 
        MemoryUsage,
        Traffic,
        Indexing,
        
        DatabaseStorageUsage,
        DatabaseTraffic,
        DatabaseIndexing
    }
}

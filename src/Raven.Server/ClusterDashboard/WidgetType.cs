// -----------------------------------------------------------------------
//  <copyright file="WidgetType.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

namespace Raven.Server.ClusterDashboard
{
    public enum WidgetType
    {
        Unknown,
        Debug, // TODO: fake widget used for debugging studio
        CpuUsage,
        License,
        MemoryUsage,
        Traffic,
    }
}

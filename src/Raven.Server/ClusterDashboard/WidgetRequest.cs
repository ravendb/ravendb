// -----------------------------------------------------------------------
//  <copyright file="WidgetRequest.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

namespace Raven.Server.ClusterDashboard
{
    public class WidgetRequest
    {
        public string Command { get; set; }
        public int Id { get; set; }
        public WidgetType Type { get; set; }
        public object Config { get; set; }
    }
}

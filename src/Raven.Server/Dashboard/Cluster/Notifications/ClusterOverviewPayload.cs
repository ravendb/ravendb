// -----------------------------------------------------------------------
//  <copyright file="ClusterOverviewPayload.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using Raven.Client.ServerWide;
using Sparrow.Json.Parsing;

namespace Raven.Server.Dashboard.Cluster.Notifications
{
    public class ClusterOverviewPayload : AbstractClusterDashboardNotification
    {
        public string NodeTag { get; set; }
        public string NodeUrl { get; set; }
        
        public string NodeType { get; set; }
        public RachisState NodeState { get; set; }
        
        public DateTime StartTime { get; set; }

        public override ClusterDashboardNotificationType Type => ClusterDashboardNotificationType.ClusterOverview;
        
        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();

            json[nameof(NodeTag)] = NodeTag;
            json[nameof(NodeUrl)] = NodeUrl;
            json[nameof(NodeType)] = NodeType;
            json[nameof(NodeState)] = NodeState;
            json[nameof(StartTime)] = StartTime;

            return json;
        }
        
        public override DynamicJsonValue ToJsonWithFilter(CanAccessDatabase filter)
        {
            return ToJson();
        }
    }
}

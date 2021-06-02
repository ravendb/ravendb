// -----------------------------------------------------------------------
//  <copyright file="CpuUsagePayload.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Sparrow.Json.Parsing;

namespace Raven.Server.Dashboard.Cluster.Notifications
{
    public class CpuUsagePayload : AbstractClusterDashboardNotification
    {
        public int MachineCpuUsage { get; set; }
        public int ProcessCpuUsage { get; set; }
        public int UtilizedCores { get; set; }
        public int NumberOfCores { get; set; }

        public override ClusterDashboardNotificationType Type => ClusterDashboardNotificationType.CpuUsage;

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();

            json[nameof(ProcessCpuUsage)] = ProcessCpuUsage;
            json[nameof(MachineCpuUsage)] = MachineCpuUsage;
            json[nameof(UtilizedCores)] = UtilizedCores;
            json[nameof(NumberOfCores)] = NumberOfCores;

            return json;
        }

        public override DynamicJsonValue ToJsonWithFilter(CanAccessDatabase filter)
        {
            return ToJson();
        }
    }
}

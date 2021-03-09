// -----------------------------------------------------------------------
//  <copyright file="CpuUsagePayload.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using Sparrow.Json.Parsing;

namespace Raven.Server.ClusterDashboard.Widgets
{
    public class CpuUsagePayload : IDynamicJson
    {
        public int MachineCpuUsage { get; set; }
        public int ProcessCpuUsage { get; set; }
        public int UtilizedCores { get; set; }
        public int NumberOfCores { get; set; }
        public DateTime Time { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(ProcessCpuUsage)] = ProcessCpuUsage,
                [nameof(MachineCpuUsage)] = MachineCpuUsage,
                [nameof(UtilizedCores)] = UtilizedCores,
                [nameof(NumberOfCores)] = NumberOfCores,
                [nameof(Time)] = Time
            };
        }
    }
}

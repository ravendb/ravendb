// -----------------------------------------------------------------------
//  <copyright file="ServerCpu.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Lextm.SharpSnmpLib;

using Raven.Database.Config;

namespace Raven.Database.Plugins.Builtins.Monitoring.Snmp.Objects.Server
{
    public class ServerCpu : ScalarObjectBase<Gauge32>
    {
        public ServerCpu()
            : base("1.7")
        {
        }

        protected override Gauge32 GetData()
        {
            return new Gauge32((int)(CpuStatistics.Average * 100));
        }
    }
}

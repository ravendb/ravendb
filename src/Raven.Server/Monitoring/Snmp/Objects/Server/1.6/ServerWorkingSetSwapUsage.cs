using System;
using Lextm.SharpSnmpLib;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.LowMemory;
using Sparrow.Platform;
using Sparrow.Platform.Posix;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class ServerWorkingSetSwapUsage : ScalarObjectBase<Gauge32>
    {
        private readonly MetricCacher _metricCacher;

        public ServerWorkingSetSwapUsage(MetricCacher metricCacher)
            : base(SnmpOids.Server.WorkingSetSwapUsage)
        {
            _metricCacher = metricCacher;
        }

        protected override Gauge32 GetData()
        {
            return new Gauge32(_metricCacher.GetValue<MemoryInfoResult>(
                MetricCacher.Keys.Server.MemoryInfoExtended.RefreshRate15Seconds).WorkingSetSwapUsage.GetValue(SizeUnit.Megabytes));
        }
    }
}

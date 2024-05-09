using System.Collections.Generic;
using System.Diagnostics.Metrics;
using Lextm.SharpSnmpLib;
using Raven.Server.Monitoring.OpenTelemetry;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    internal sealed class ServerUpTime : ScalarObjectBase<TimeTicks>, ITaggedMetricInstrument<long>
    {
        private readonly ServerStatistics _statistics;

        public ServerUpTime(ServerStatistics statistics)
            : base(SnmpOids.Server.UpTime)
        {
            _statistics = statistics;
        }

        protected override TimeTicks GetData()
        {
            return SnmpValuesHelper.TimeSpanToTimeTicks(_statistics.UpTime);
        }

        public Measurement<long> GetCurrentValue()
        {
            return new((long)_statistics.UpTime.TotalMilliseconds, new KeyValuePair<string, object>("b", 1));
        }
    }

    internal sealed class ServerUpTimeGlobal : ScalarObjectBase<TimeTicks>, IMetricInstrument<long>
    {
        private readonly ServerStatistics _statistics;

        public ServerUpTimeGlobal(ServerStatistics statistics)
            : base(SnmpOids.Server.UpTimeGlobal, appendRoot: false)
        {
            _statistics = statistics;
        }

        protected override TimeTicks GetData()
        {
            return SnmpValuesHelper.TimeSpanToTimeTicks(_statistics.UpTime);
        }

        public long GetCurrentValue()
        {
            return (long)_statistics.UpTime.TotalMilliseconds;
        }
    }
}

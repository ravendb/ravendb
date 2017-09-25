using Lextm.SharpSnmpLib;
using Raven.Server.Utils;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class ServerRequestsPerSecond : ScalarObjectBase<Gauge32>
    {
        private readonly MetricCounters _metrics;

        public ServerRequestsPerSecond(MetricCounters metrics)
            : base(SnmpOids.Server.RequestsPerSecond)
        {
            _metrics = metrics;
        }

        protected override Gauge32 GetData()
        {
            return new Gauge32((int)_metrics.Requests.RequestsPerSec.OneMinuteRate);
        }
    }
}

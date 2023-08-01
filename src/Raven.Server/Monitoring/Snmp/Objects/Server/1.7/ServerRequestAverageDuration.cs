using Lextm.SharpSnmpLib;
using Raven.Server.Utils;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public sealed class ServerRequestAverageDuration : ScalarObjectBase<Gauge32>
    {
        private readonly MetricCounters _metrics;

        public ServerRequestAverageDuration(MetricCounters metrics)
            : base(SnmpOids.Server.RequestAverageDuration)
        {
            _metrics = metrics;
        }

        protected override Gauge32 GetData()
        {
            return new Gauge32((int)_metrics.Requests.AverageDuration.GetRate());
        }
    }
}

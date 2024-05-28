using Lextm.SharpSnmpLib;
using Raven.Server.Monitoring.OpenTelemetry;
using Raven.Server.Utils;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public sealed class ServerRequestAverageDuration : ScalarObjectBase<Gauge32>, IMetricInstrument<int>
    {
        private readonly MetricCounters _metrics;

        public ServerRequestAverageDuration(MetricCounters metrics)
            : base(SnmpOids.Server.RequestAverageDuration)
        {
            _metrics = metrics;
        }

        private int Value => (int)_metrics.Requests.AverageDuration.GetRate();
        
        protected override Gauge32 GetData()
        {
            return new Gauge32(Value);
        }

        public int GetCurrentMeasurement() => Value;
    }
}

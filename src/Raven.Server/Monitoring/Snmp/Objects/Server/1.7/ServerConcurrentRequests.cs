using Lextm.SharpSnmpLib;
using Raven.Server.Monitoring.OpenTelemetry;
using Raven.Server.Utils;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public sealed class ServerConcurrentRequests(MetricCounters metrics) : ScalarObjectBase<Integer32>(SnmpOids.Server.ConcurrentRequests), IMetricInstrument<int>
    {
        private int Value => (int)metrics.Requests.ConcurrentRequestsCount;
        
        protected override Integer32 GetData()
        {
            return new Integer32(Value);
        }

        public int GetCurrentMeasurement() => Value;
    }
}

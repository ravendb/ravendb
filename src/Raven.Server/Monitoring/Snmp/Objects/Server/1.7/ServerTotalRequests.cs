using Lextm.SharpSnmpLib;
using Raven.Server.Monitoring.OpenTelemetry;
using Raven.Server.Utils;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public sealed class ServerTotalRequests(MetricCounters metrics) : ScalarObjectBase<Integer32>(SnmpOids.Server.TotalRequests), IMetricInstrument<int>
    {
        private int Value => (int)metrics.Requests.RequestsPerSec.Count;
        
        protected override Integer32 GetData()
        {
            return new Integer32(Value);
        }

        public int GetCurrentMeasurement() => Value;
    }
}

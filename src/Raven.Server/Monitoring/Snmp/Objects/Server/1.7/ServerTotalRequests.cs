using Lextm.SharpSnmpLib;
using Raven.Server.Utils;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class ServerTotalRequests : ScalarObjectBase<Integer32>
    {
        private readonly MetricCounters _metrics;

        public ServerTotalRequests(MetricCounters metrics)
            : base(SnmpOids.Server.TotalRequests)
        {
            _metrics = metrics;
        }

        protected override Integer32 GetData()
        {
            return new Integer32((int)_metrics.Requests.RequestsPerSec.Count);
        }
    }
}

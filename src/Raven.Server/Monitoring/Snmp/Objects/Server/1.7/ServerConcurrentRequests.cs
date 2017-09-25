using Lextm.SharpSnmpLib;
using Raven.Server.Utils;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class ServerConcurrentRequests : ScalarObjectBase<Integer32>
    {
        private readonly MetricCounters _metrics;

        public ServerConcurrentRequests(MetricCounters metrics)
            : base(SnmpOids.Server.ConcurrentRequests)
        {
            _metrics = metrics;
        }

        protected override Integer32 GetData()
        {
            return new Integer32((int)_metrics.Requests.ConcurrentRequestsCount);
        }
    }
}

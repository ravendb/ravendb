using Lextm.SharpSnmpLib;
using Raven.Server.Utils;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class ServerConcurrentRequests : ScalarObjectBase<Integer32>
    {
        private readonly MetricsCountersManager _metrics;

        public ServerConcurrentRequests(MetricsCountersManager metrics)
            : base("1.7.1")
        {
            _metrics = metrics;
        }

        protected override Integer32 GetData()
        {
            return new Integer32((int)_metrics.ConcurrentRequestsCount);
        }
    }
}

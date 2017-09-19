using Lextm.SharpSnmpLib;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class ServerConcurrentRequests : ScalarObjectBase<Integer32>
    {
        private readonly RavenServer _server;

        public ServerConcurrentRequests(RavenServer server)
            : base("1.7.1")
        {
            _server = server;
        }

        protected override Integer32 GetData()
        {
            return new Integer32((int)_server.Metrics.ConcurrentRequestsCount);
        }
    }
}

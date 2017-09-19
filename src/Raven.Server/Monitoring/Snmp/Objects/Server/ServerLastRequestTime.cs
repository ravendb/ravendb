using Lextm.SharpSnmpLib;
using Raven.Client.Util;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    internal class ServerLastRequestTime : ScalarObjectBase<TimeTicks>
    {
        private readonly RavenServer _server;

        public ServerLastRequestTime(RavenServer server)
            : base("1.8")
        {
            _server = server;
        }

        protected override TimeTicks GetData()
        {
            if (_server.Statistics.LastRequestTime.HasValue)
                return new TimeTicks(SystemTime.UtcNow - _server.Statistics.LastRequestTime.Value);

            return null;
        }
    }
}

using Lextm.SharpSnmpLib;
using Raven.Client.Util;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    internal class ServerLastRequestTime : ScalarObjectBase<TimeTicks>
    {
        private readonly ServerStatistics _statistics;

        public ServerLastRequestTime(ServerStatistics statistics)
            : base("1.8")
        {
            _statistics = statistics;
        }

        protected override TimeTicks GetData()
        {
            if (_statistics.LastRequestTime.HasValue)
                return new TimeTicks(SystemTime.UtcNow - _statistics.LastRequestTime.Value);

            return null;
        }
    }
}

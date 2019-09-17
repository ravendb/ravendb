using Lextm.SharpSnmpLib;
using Raven.Client.Util;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    internal class ServerLastUserRequestTime : ScalarObjectBase<TimeTicks>
    {
        private readonly ServerStatistics _statistics;

        public ServerLastUserRequestTime(ServerStatistics statistics)
            : base(SnmpOids.Server.LastUserRequestTime)
        {
            _statistics = statistics;
        }

        protected override TimeTicks GetData()
        {
            if (_statistics.LastUserRequestTime.HasValue)
                return new TimeTicks(SystemTime.UtcNow - _statistics.LastUserRequestTime.Value);

            return null;
        }
    }
}

using Lextm.SharpSnmpLib;
using Raven.Client.Util;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    internal class ServerLastAuthorizedNonClusterAdminRequestTime : ScalarObjectBase<TimeTicks>
    {
        private readonly ServerStatistics _statistics;

        public ServerLastAuthorizedNonClusterAdminRequestTime(ServerStatistics statistics)
            : base(SnmpOids.Server.LastAuthorizedNonClusterAdminRequestTime)
        {
            _statistics = statistics;
        }

        protected override TimeTicks GetData()
        {
            if (_statistics.LastAuthorizedNonClusterAdminRequestTime.HasValue)
                return new TimeTicks(SystemTime.UtcNow - _statistics.LastAuthorizedNonClusterAdminRequestTime.Value);

            return new TimeTicks(uint.MaxValue);
        }
    }
}

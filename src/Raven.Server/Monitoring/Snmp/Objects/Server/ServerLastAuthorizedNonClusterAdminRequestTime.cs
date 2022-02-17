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
                return SnmpValuesHelper.TimeSpanToTimeTicks(SystemTime.UtcNow - _statistics.LastAuthorizedNonClusterAdminRequestTime.Value);

            return SnmpValuesHelper.TimeTicksMax;
        }
    }
}

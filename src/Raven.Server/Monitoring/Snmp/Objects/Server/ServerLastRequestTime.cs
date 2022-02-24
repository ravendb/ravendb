using Lextm.SharpSnmpLib;
using Raven.Client.Util;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    internal class ServerLastRequestTime : ScalarObjectBase<TimeTicks>
    {
        private readonly ServerStatistics _statistics;

        public ServerLastRequestTime(ServerStatistics statistics)
            : base(SnmpOids.Server.LastRequestTime)
        {
            _statistics = statistics;
        }

        protected override TimeTicks GetData()
        {
            if (_statistics.LastRequestTime.HasValue)
                return SnmpValuesHelper.TimeSpanToTimeTicks(SystemTime.UtcNow - _statistics.LastRequestTime.Value);

            return null;
        }
    }
}

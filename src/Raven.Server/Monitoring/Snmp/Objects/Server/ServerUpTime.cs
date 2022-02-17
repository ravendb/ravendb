using Lextm.SharpSnmpLib;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    internal class ServerUpTime : ScalarObjectBase<TimeTicks>
    {
        private readonly ServerStatistics _statistics;

        public ServerUpTime(ServerStatistics statistics)
            : base(SnmpOids.Server.UpTime)
        {
            _statistics = statistics;
        }

        protected override TimeTicks GetData()
        {
            return SnmpValuesHelper.TimeSpanToTimeTicks(_statistics.UpTime);
        }
    }

    internal class ServerUpTimeGlobal : ScalarObjectBase<TimeTicks>
    {
        private readonly ServerStatistics _statistics;

        public ServerUpTimeGlobal(ServerStatistics statistics)
            : base(SnmpOids.Server.UpTimeGlobal, appendRoot: false)
        {
            _statistics = statistics;
        }

        protected override TimeTicks GetData()
        {
            return SnmpValuesHelper.TimeSpanToTimeTicks(_statistics.UpTime);
        }
    }
}

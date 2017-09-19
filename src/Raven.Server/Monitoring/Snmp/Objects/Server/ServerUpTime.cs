using System;
using Lextm.SharpSnmpLib;
using Raven.Client.Util;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    internal class ServerUpTime : ScalarObjectBase<TimeTicks>
    {
        private readonly DateTime _startUpTime;

        public ServerUpTime(ServerStatistics statistics)
            : base("1.3")
        {
            _startUpTime = statistics.StartUpTime;
        }

        protected override TimeTicks GetData()
        {
            return new TimeTicks(SystemTime.UtcNow - _startUpTime);
        }
    }

    internal class ServerUpTimeGlobal : ScalarObjectBase<TimeTicks>
    {
        private readonly DateTime _startUpTime;

        public ServerUpTimeGlobal(ServerStatistics statistics)
            : base("1.3.6.1.2.1.1.3.0")
        {
            _startUpTime = statistics.StartUpTime;
        }

        protected override TimeTicks GetData()
        {
            return new TimeTicks(SystemTime.UtcNow - _startUpTime);
        }
    }
}

using Lextm.SharpSnmpLib;
using Sparrow.Server.Extensions;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class TcpActiveConnections : ScalarObjectBase<Gauge32>
    {
        public TcpActiveConnections()
            : base(SnmpOids.Server.TcpActiveConnections)
        {
        }

        protected override Gauge32 GetData()
        {
            var properties = TcpExtensions.GetIPGlobalPropertiesSafely();
            var ipv4Stats = properties.GetTcpIPv4StatisticsSafely();
            var ipv6Stats = properties.GetTcpIPv6StatisticsSafely();

            var currentIpv4Connections = ipv4Stats.GetCurrentConnectionsSafely() ?? 0;
            var currentIpv6Connections = ipv6Stats.GetCurrentConnectionsSafely() ?? 0;

            return new Gauge32(currentIpv4Connections + currentIpv6Connections);
        }
    }
}

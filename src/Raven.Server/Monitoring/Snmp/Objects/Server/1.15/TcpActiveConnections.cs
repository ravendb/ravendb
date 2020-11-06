using System;
using System.Net.NetworkInformation;
using Lextm.SharpSnmpLib;

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
            var properties = IPGlobalProperties.GetIPGlobalProperties();
            var ipv4Stats = GetTcpStatisticsSafely(() => properties.GetTcpIPv4Statistics());
            var ipv6Stats = GetTcpStatisticsSafely(() => properties.GetTcpIPv6Statistics());

            var currentIpv4Connections = ipv4Stats?.CurrentConnections ?? 0;
            var currentIpv6Connections = ipv6Stats?.CurrentConnections ?? 0;

            return new Gauge32(currentIpv4Connections + currentIpv6Connections);
        }

        private static TcpStatistics GetTcpStatisticsSafely(Func<TcpStatistics> func)
        {
            try
            {
                return func();
            }
            catch (PlatformNotSupportedException)
            {
                return null;
            }
        }
    }
}

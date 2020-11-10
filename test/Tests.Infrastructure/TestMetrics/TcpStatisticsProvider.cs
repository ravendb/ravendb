using Sparrow.Server.Extensions;

namespace Tests.Infrastructure.TestMetrics
{
    public static class TcpStatisticsProvider
    {
        public static TcpConnections GetConnections()
        {
            var properties = TcpExtensions.GetIPGlobalPropertiesSafely();
            var ipv4Stats = properties.GetTcpIPv4StatisticsSafely();
            var ipv6Stats = properties.GetTcpIPv6StatisticsSafely();
            var currentIpv4Connections = ipv4Stats.GetCurrentConnectionsSafely() ?? 0;
            var currentIpv6Connections = ipv6Stats.GetCurrentConnectionsSafely() ?? 0;

            return new TcpConnections
            {
                CurrentIpv4 = currentIpv4Connections,
                CurrentIpv6 = currentIpv6Connections
            };
        }

        public class TcpConnections
        {
            public long CurrentIpv4 { get; set; }
            public long CurrentIpv6 { get; set; }
        }
    }
}

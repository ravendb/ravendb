using Lextm.SharpSnmpLib;
using Raven.Server.Monitoring.OpenTelemetry;
using Sparrow.Server.Extensions;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public sealed class TcpActiveConnections : ScalarObjectBase<Gauge32>, IMetricInstrument<long>
    {
        public TcpActiveConnections()
            : base(SnmpOids.Server.TcpActiveConnections)
        {
        }

        private long Value
        {
            get
            {
                var properties = TcpExtensions.GetIPGlobalPropertiesSafely();
                var ipv4Stats = properties.GetTcpIPv4StatisticsSafely();
                var ipv6Stats = properties.GetTcpIPv6StatisticsSafely();

                var currentIpv4Connections = ipv4Stats.GetCurrentConnectionsSafely() ?? 0;
                var currentIpv6Connections = ipv6Stats.GetCurrentConnectionsSafely() ?? 0;

                return (currentIpv4Connections + currentIpv6Connections);
            }
        }

        protected override Gauge32 GetData()
        {
            return new Gauge32(Value);
        }

        public long GetCurrentMeasurement() => Value;
    }
}

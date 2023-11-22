using System;
using Lextm.SharpSnmpLib;
using Raven.Server.Utils;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public sealed class ServerRequestsPerSecond : ScalarObjectBase<Gauge32>
    {
        private readonly MetricCounters _metrics;
        private readonly RequestRateType _requestRateType;

        public ServerRequestsPerSecond(MetricCounters metrics, RequestRateType requestRateType)
            : base(GetOid(requestRateType))
        {
            _metrics = metrics;
            _requestRateType = requestRateType;
        }

        protected override Gauge32 GetData()
        {
            return _requestRateType switch
            {
                RequestRateType.OneMinute => new Gauge32((int)_metrics.Requests.RequestsPerSec.OneMinuteRate),
                RequestRateType.FiveSeconds => new Gauge32((int)_metrics.Requests.RequestsPerSec.FiveSecondRate),
                _ => throw new ArgumentOutOfRangeException(nameof(_requestRateType), _requestRateType, null)
            };
        }

        private static string GetOid(RequestRateType requestRateType)
        {
            return requestRateType switch
            {
                RequestRateType.OneMinute => SnmpOids.Server.RequestsPerSecond1M,
                RequestRateType.FiveSeconds => SnmpOids.Server.RequestsPerSecond5S,
                _ => throw new ArgumentOutOfRangeException(nameof(requestRateType), requestRateType, null)
            };
        }

        public enum RequestRateType
        {
            OneMinute,
            FiveSeconds
        }
    }
}

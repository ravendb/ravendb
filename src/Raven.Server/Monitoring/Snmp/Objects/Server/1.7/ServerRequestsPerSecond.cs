using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using Lextm.SharpSnmpLib;
using Raven.Server.Monitoring.OpenTelemetry;
using Raven.Server.Utils;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public sealed class ServerRequestsPerSecond(MetricCounters metrics, ServerRequestsPerSecond.RequestRateType requestRateType)
        : ScalarObjectBase<Gauge32>(GetOid(requestRateType)), ITaggedMetricInstrument<int>
    {
        private int Value
        {
            get
            {
                return requestRateType switch
                {
                    RequestRateType.OneMinute => (int)metrics.Requests.RequestsPerSec.OneMinuteRate,
                    RequestRateType.FiveSeconds => (int)metrics.Requests.RequestsPerSec.FiveSecondRate,
                    _ => throw new ArgumentOutOfRangeException(nameof(requestRateType), requestRateType, null)
                };
            }
        }
        
        public Measurement<int> GetCurrentMeasurement() => new(Value, new KeyValuePair<string, object>("RateType", requestRateType));
        
        protected override Gauge32 GetData()
        {
            return new Gauge32(Value);
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

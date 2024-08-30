using System.Collections.Generic;
using System.Diagnostics.Metrics;
using Lextm.SharpSnmpLib;
using Raven.Server.Monitoring.OpenTelemetry;
using Sparrow;
using Voron.Impl;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public sealed class ServerEncryptionBuffersMemoryInPool() : ScalarObjectBase<Gauge32>(SnmpOids.Server.EncryptionBuffersMemoryInPool), IMetricInstrument<long>
    {
        private long Value
        {
            get
            {
                var encryptionBuffers = EncryptionBuffersPool.Instance.GetStats();
                return new Size(encryptionBuffers.TotalPoolSize, SizeUnit.Bytes).GetValue(SizeUnit.Megabytes);
            }
        }
        
        protected override Gauge32 GetData()
        {
            return new Gauge32(Value);
        }

        public long GetCurrentMeasurement() => Value;
    }
}

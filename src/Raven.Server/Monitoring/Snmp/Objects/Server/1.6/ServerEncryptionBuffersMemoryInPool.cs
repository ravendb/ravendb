using System.Collections.Generic;
using System.Diagnostics.Metrics;
using Lextm.SharpSnmpLib;
using Raven.Server.Monitoring.OpenTelemetry;
using Sparrow;
using Voron.Impl;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public sealed class ServerEncryptionBuffersMemoryInPool : ScalarObjectBase<Gauge32>, ITaggedMetricInstrument<long>
    {
        private readonly KeyValuePair<string, object> _nodeTag;

        public ServerEncryptionBuffersMemoryInPool(KeyValuePair<string, object> nodeTag = default) : base(SnmpOids.Server.EncryptionBuffersMemoryInPool)
        {
            _nodeTag = nodeTag;
        }

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

        public Measurement<long> GetCurrentValue()
        {
            return new(Value, _nodeTag);
        }
    }
}

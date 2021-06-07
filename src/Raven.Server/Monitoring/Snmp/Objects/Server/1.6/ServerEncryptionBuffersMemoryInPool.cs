using Lextm.SharpSnmpLib;
using Sparrow;
using Sparrow.LowMemory;
using Voron.Impl;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class ServerEncryptionBuffersMemoryInPool : ScalarObjectBase<Gauge32>
    {
        public ServerEncryptionBuffersMemoryInPool() : base(SnmpOids.Server.EncryptionBuffersMemoryInPool)
        {
        }

        protected override Gauge32 GetData()
        {
            var encryptionBuffers = EncryptionBuffersPool.Instance.GetStats();

            return new Gauge32(new Size(encryptionBuffers.TotalPoolSize, SizeUnit.Bytes).GetValue(SizeUnit.Megabytes));
        }
    }
}

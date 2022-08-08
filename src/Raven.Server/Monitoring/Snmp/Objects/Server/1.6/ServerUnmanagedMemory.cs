using Lextm.SharpSnmpLib;
using Sparrow;
using Sparrow.LowMemory;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class ServerUnmanagedMemory : ScalarObjectBase<Gauge32>
    {
        public ServerUnmanagedMemory() : base(SnmpOids.Server.UnmanagedMemory)
        {
        }

        protected override Gauge32 GetData()
        {
            var unmanagedMemoryInBytes = AbstractLowMemoryMonitor.GetUnmanagedAllocationsInBytes();

            return new Gauge32(new Size(unmanagedMemoryInBytes, SizeUnit.Bytes).GetValue(SizeUnit.Megabytes));
        }
    }
}

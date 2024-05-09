using Lextm.SharpSnmpLib;
using Sparrow;
using Sparrow.Server.LowMemory;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public sealed class ServerDirtyMemory : ScalarObjectBase<Gauge32>
    {
        public ServerDirtyMemory() : base(SnmpOids.Server.DirtyMemory)
        {
        }

        protected override Gauge32 GetData()
        {
            return new Gauge32(MemoryInformation.GetDirtyMemoryState().TotalDirty.GetValue(SizeUnit.Megabytes));
        }
    }
}

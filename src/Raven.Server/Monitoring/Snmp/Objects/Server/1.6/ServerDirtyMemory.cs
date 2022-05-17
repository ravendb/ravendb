using Lextm.SharpSnmpLib;
using Sparrow;
using Sparrow.LowMemory;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class ServerDirtyMemory : ScalarObjectBase<Gauge32>
    {
        public ServerDirtyMemory() : base(SnmpOids.Server.DirtyMemory)
        {
        }

        protected override Gauge32 GetData()
        {
            var totalDirtyInBytes = MemoryInformation.GetDirtyMemoryState().TotalDirtyInBytes;
            return new Gauge32(new Size(totalDirtyInBytes, SizeUnit.Bytes).GetValue(SizeUnit.Megabytes));
        }
    }
}

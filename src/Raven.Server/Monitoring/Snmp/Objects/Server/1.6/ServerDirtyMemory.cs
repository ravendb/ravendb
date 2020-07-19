using Lextm.SharpSnmpLib;
using Raven.Server.Utils;
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
            return new Gauge32(MemoryInformation.GetDirtyMemoryState().TotalDirtyInMb);
        }
    }
}

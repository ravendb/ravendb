using System.Globalization;
using Lextm.SharpSnmpLib;
using Sparrow.LowMemory;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class ServerLowMemoryFlag : ScalarObjectBase<OctetString>
    {
        public ServerLowMemoryFlag()
            : base(SnmpOids.Server.LowMemoryFlag)
        {
        }

        protected override OctetString GetData()
        {
            return new OctetString(LowMemoryNotification.Instance.LowMemoryState.ToString(CultureInfo.InvariantCulture));
        }
    }
}

using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Globalization;
using Lextm.SharpSnmpLib;
using Raven.Server.Monitoring.OpenTelemetry;
using Sparrow.LowMemory;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public sealed class ServerLowMemoryFlag() : ScalarObjectBase<OctetString>(SnmpOids.Server.LowMemoryFlag), IMetricInstrument<byte>
    {
        protected override OctetString GetData()
        {
            return new OctetString(LowMemoryNotification.Instance.LowMemoryState.ToString(CultureInfo.InvariantCulture));
        }

        public byte GetCurrentMeasurement() => (byte)(LowMemoryNotification.Instance.LowMemoryState ? 1 : 0);
    }
}

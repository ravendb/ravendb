using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Globalization;
using Lextm.SharpSnmpLib;
using Raven.Server.Monitoring.OpenTelemetry;
using Sparrow.LowMemory;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public sealed class ServerLowMemoryFlag : ScalarObjectBase<OctetString>, ITaggedMetricInstrument<byte>
    {
        private readonly KeyValuePair<string, object> _nodeTag;

        public ServerLowMemoryFlag(KeyValuePair<string, object> nodeTag = default)
            : base(SnmpOids.Server.LowMemoryFlag)
        {
            _nodeTag = nodeTag;
        }

        protected override OctetString GetData()
        {
            return new OctetString(LowMemoryNotification.Instance.LowMemoryState.ToString(CultureInfo.InvariantCulture));
        }

        public Measurement<byte> GetCurrentValue()
        {
            return new((byte)(LowMemoryNotification.Instance.LowMemoryState ? 1 : 0), _nodeTag);
        }
    }
}

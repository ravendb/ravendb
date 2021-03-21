using System;
using System.Globalization;
using Lextm.SharpSnmpLib;
using Raven.Server.Utils;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class ServerGcCompacted : ServerGcBase<OctetString>
    {
        public ServerGcCompacted(MetricCacher metricCacher, GCKind gcKind)
            : base(metricCacher, gcKind, SnmpOids.Server.GcCompacted)
        {
        }

        public override ISnmpData Data => new OctetString(GetGCMemoryInfo().Compacted.ToString(CultureInfo.InvariantCulture));

        protected override OctetString GetData()
        {
            throw new NotSupportedException();
        }
    }
}

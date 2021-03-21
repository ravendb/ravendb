using System;
using System.Globalization;
using Lextm.SharpSnmpLib;
using Raven.Server.Utils;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class ServerGcConcurrent : ServerGcBase<OctetString>
    {
        public ServerGcConcurrent(MetricCacher metricCacher, GCKind gcKind)
            : base(metricCacher, gcKind, SnmpOids.Server.GcConcurrent)
        {
        }

        public override ISnmpData Data => new OctetString(GetGCMemoryInfo().Concurrent.ToString(CultureInfo.InvariantCulture));

        protected override OctetString GetData()
        {
            throw new NotSupportedException();
        }
    }
}

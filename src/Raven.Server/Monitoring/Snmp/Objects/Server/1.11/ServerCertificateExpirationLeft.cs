using System;
using Lextm.SharpSnmpLib;
using Raven.Client.Util;
using Raven.Server.Monitoring.OpenTelemetry;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public sealed class ServerCertificateExpirationLeft : ScalarObjectBase<TimeTicks>, IMetricInstrument<int>
    {
        private readonly ServerStore _store;

        public ServerCertificateExpirationLeft(ServerStore store)
            : base(SnmpOids.Server.ServerCertificateExpirationLeft)
        {
            _store = store;
        }

        protected override TimeTicks GetData()
        {
            var holder = _store.Server.Certificate;
            if (holder == null || holder.Certificate == null)
                return null;

            var notAfter = holder.Certificate.NotAfter;

            var timeLeft = notAfter - SystemTime.UtcNow;
            return SnmpValuesHelper.TimeSpanToTimeTicks(timeLeft.TotalMilliseconds > 0 ? timeLeft : TimeSpan.Zero);
        }

        public int GetCurrentMeasurement()
        {
            var holder = _store.Server.Certificate;
            if (holder == null || holder.Certificate == null)
                return 0;

            var notAfter = holder.Certificate.NotAfter;

            var timeLeft = notAfter - SystemTime.UtcNow;
            return (int)(timeLeft.TotalMilliseconds > 0 ? timeLeft : TimeSpan.Zero).TotalSeconds;
        }
    }
}

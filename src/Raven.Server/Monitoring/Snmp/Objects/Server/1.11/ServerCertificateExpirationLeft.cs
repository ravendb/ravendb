using System;
using Lextm.SharpSnmpLib;
using Raven.Client.Util;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class ServerCertificateExpirationLeft : ScalarObjectBase<TimeTicks>
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
    }
}

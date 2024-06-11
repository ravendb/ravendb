using System;
using Lextm.SharpSnmpLib;
using Raven.Client.Util;
using Raven.Server.Monitoring.OpenTelemetry;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public sealed class ServerLicenseExpirationLeft : ScalarObjectBase<TimeTicks>, IMetricInstrument<int>
    {
        private readonly ServerStore _store;

        public ServerLicenseExpirationLeft(ServerStore store)
            : base(SnmpOids.Server.ServerLicenseExpirationLeft)
        {
            _store = store;
        }

        private TimeSpan? Value
        {
            get
            {
                var status = _store.LicenseManager.LicenseStatus;
                if (status.Expiration.HasValue == false)
                    return null;

                return status.Expiration.Value - SystemTime.UtcNow;
            }
        }

        protected override TimeTicks GetData()
        {
            var timeLeft = Value ?? default;
            return SnmpValuesHelper.TimeSpanToTimeTicks(timeLeft.TotalMilliseconds > 0 ? timeLeft : TimeSpan.Zero);
        }

        public int GetCurrentMeasurement()
        {
            return (int)(Value?.TotalSeconds ?? 0);
        }
    }
}

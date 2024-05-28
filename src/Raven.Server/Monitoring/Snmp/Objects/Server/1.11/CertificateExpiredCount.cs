using Lextm.SharpSnmpLib;
using Raven.Server.Monitoring.OpenTelemetry;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public sealed class CertificateExpiredCount : ScalarObjectBase<Integer32>, IMetricInstrument<int>
    {
        private readonly ServerStore _store;

        public CertificateExpiredCount(ServerStore store)
            : base(SnmpOids.Server.CertificateExpiredCount)
        {
            _store = store;
        }

        private int Value
        {
            get
            {
                var count = 0;
                var now = _store.Server.Time.GetUtcNow();

                foreach (var notAfter in CertificateExpiringCount.GetAllCertificateExpirationDates(_store))
                {
                    if (now > notAfter)
                        count++;
                }

                return count;
            }
        }

        protected override Integer32 GetData()
        {
            return new Integer32(Value);
        }

        public int GetCurrentMeasurement() => Value;
    }
}

using Lextm.SharpSnmpLib;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public sealed class CertificateExpiredCount : ScalarObjectBase<Integer32>
    {
        private readonly ServerStore _store;

        public CertificateExpiredCount(ServerStore store)
            : base(SnmpOids.Server.CertificateExpiredCount)
        {
            _store = store;
        }

        protected override Integer32 GetData()
        {
            var count = 0;
            var now = _store.Server.Time.GetUtcNow();

            foreach (var notAfter in CertificateExpiringCount.GetAllCertificateExpirationDates(_store))
            {
                if (now > notAfter)
                    count++;
            }

            return new Integer32(count);
        }
    }
}

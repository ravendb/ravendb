using System;
using System.Collections.Generic;
using Lextm.SharpSnmpLib;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server.Monitoring.OpenTelemetry;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public sealed class CertificateExpiringCount : ScalarObjectBase<Integer32>, IMetricInstrument<int>
    {
        private readonly ServerStore _store;

        public CertificateExpiringCount(ServerStore store)
            : base(SnmpOids.Server.CertificateExpiringCount)
        {
            _store = store;
        }

        private int Value
        {
            get
            {
                var count = 0;
                var now = _store.Server.Time.GetUtcNow();
                var expiringThreshold = _store.Server.Configuration.Security.CertificateExpiringThreshold.AsTimeSpan;

                foreach (var notAfter in GetAllCertificateExpirationDates(_store))
                {
                    if (now > notAfter)
                        continue; // we do not want to count already expired certificates

                    if (now > notAfter.Subtract(expiringThreshold))
                        count++;
                }

                return count;
            }
        }

        protected override Integer32 GetData()
        {
            return new Integer32(Value);
        }

        public static IEnumerable<DateTime> GetAllCertificateExpirationDates(ServerStore store)
        {
            using (store.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {

                foreach (var (_, json) in ClusterStateMachine.GetAllCertificatesFromCluster(context, 0, int.MaxValue))
                {
                    if (json.TryGet(nameof(CertificateDefinition.NotAfter), out DateTime notAfter))
                        yield return notAfter;
                }
            }
        }

        public int GetCurrentMeasurement() => Value;
    }
}

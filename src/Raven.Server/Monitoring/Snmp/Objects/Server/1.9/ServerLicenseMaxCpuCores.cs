using Lextm.SharpSnmpLib;
using Raven.Server.Monitoring.OpenTelemetry;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public sealed class ServerLicenseMaxCpuCores : ScalarObjectBase<Integer32>, IMetricInstrument<int>
    {
        private readonly ServerStore _store;

        public ServerLicenseMaxCpuCores(ServerStore store)
            : base(SnmpOids.Server.ServerLicenseMaxCpuCores)
        {
            _store = store;
        }

        private int Value => _store.LicenseManager.LicenseStatus.MaxCores;
        
        protected override Integer32 GetData()
        {
            return new Integer32(Value);
        }

        public int GetCurrentMeasurement() => Value;
    }
}

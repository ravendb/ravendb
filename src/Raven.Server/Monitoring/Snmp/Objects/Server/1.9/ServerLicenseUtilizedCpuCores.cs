using Lextm.SharpSnmpLib;
using Raven.Server.Monitoring.OpenTelemetry;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public sealed class ServerLicenseUtilizedCpuCores : ScalarObjectBase<Integer32>, IMetricInstrument<int>
    {
        private readonly ServerStore _store;

        public ServerLicenseUtilizedCpuCores(ServerStore store)
            : base(SnmpOids.Server.ServerLicenseUtilizedCpuCores)
        {
            _store = store;
        }

        private int Value => _store.LicenseManager.GetCoresLimitForNode(out _);
        
        protected override Integer32 GetData()
        {
            return new Integer32(Value);
        }

        public int GetCurrentMeasurement() => Value;
    }
}

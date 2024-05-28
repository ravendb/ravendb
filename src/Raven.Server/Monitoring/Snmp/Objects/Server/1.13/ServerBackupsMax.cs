using Lextm.SharpSnmpLib;
using Raven.Server.Monitoring.OpenTelemetry;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public sealed class ServerBackupsMax : ScalarObjectBase<Integer32>, IMetricInstrument<int>
    {
        private readonly ServerStore _serverStore;

        public ServerBackupsMax(ServerStore serverStore)
            : base(SnmpOids.Server.ServerBackupsMax)
        {
            _serverStore = serverStore;
        }

        private int Value => _serverStore.ConcurrentBackupsCounter.MaxNumberOfConcurrentBackups;
        
        protected override Integer32 GetData()
        {
            return new Integer32(Value);
        }

        public int GetCurrentMeasurement() => Value;
    }
}

using Lextm.SharpSnmpLib;
using Raven.Server.Monitoring.OpenTelemetry;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public sealed class ServerBackupsCurrent : ScalarObjectBase<Integer32>, IMetricInstrument<int>
    {
        private readonly ServerStore _serverStore;

        public ServerBackupsCurrent(ServerStore serverStore)
            : base(SnmpOids.Server.ServerBackupsCurrent)
        {
            _serverStore = serverStore;
        }

        private int Value => _serverStore.ConcurrentBackupsCounter.CurrentNumberOfRunningBackups;

        protected override Integer32 GetData()
        {
            return new Integer32(Value);
        }

        public int GetCurrentMeasurement() => Value;
    }
}

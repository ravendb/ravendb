using Lextm.SharpSnmpLib;
using Raven.Server.Monitoring.OpenTelemetry;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public sealed class TotalDatabaseNumberOfErrorIndexes(ServerStore serverStore)
        : DatabaseBase<Integer32>(serverStore, SnmpOids.Databases.General.TotalNumberOfErrorIndexes), IMetricInstrument<int>
    {
        private int Value
        {
            get
            {
                var count = 0;
                foreach (var database in GetLoadedDatabases())
                    count += GetCountSafely(database, DatabaseNumberOfErrorIndexes.GetCount);
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
using Lextm.SharpSnmpLib;
using Raven.Server.Documents;
using Raven.Server.Monitoring.OpenTelemetry;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public sealed class DatabaseLoadedCount : ScalarObjectBase<Integer32>, IMetricInstrument<int>
    {
        private readonly DatabasesLandlord _landlord;

        public DatabaseLoadedCount(DatabasesLandlord landlord)
            : base(SnmpOids.Databases.General.LoadedCount)
        {
            _landlord = landlord;
        }

        protected override Integer32 GetData()
        {
            return new Integer32(GetCurrentValue());
        }
        
        public int GetCurrentValue()
        {
            return _landlord.DatabasesCache.Count;
        }
    }
}

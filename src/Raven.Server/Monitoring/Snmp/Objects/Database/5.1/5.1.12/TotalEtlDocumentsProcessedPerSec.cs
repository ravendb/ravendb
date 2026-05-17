using Lextm.SharpSnmpLib;
using Raven.Server.Documents;
using Raven.Server.Documents.ETL;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public sealed class TotalEtlDocumentsProcessedPerSec : DatabaseBase<Gauge32>
    {
        public TotalEtlDocumentsProcessedPerSec(ServerStore serverStore)
            : base(serverStore, SnmpOids.Databases.General.TotalEtlDocumentsProcessedPerSec)
        {
        }

        protected override Gauge32 GetData()
        {
            var count = 0;
            foreach (var database in GetLoadedDatabases())
                count += GetCountSafely(database, GetCount);
            return new Gauge32(count);
        }

        private static int GetCount(DocumentDatabase database)
        {
            var rate = 0.0;
            foreach (EtlProcess etl in database.EtlLoader.GetEtlProcesses())
                rate += etl.Metrics.BatchSizeMeter.OneMinuteRate;
            return (int)rate;
        }
    }
}

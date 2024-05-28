using System.Diagnostics.Metrics;
using Lextm.SharpSnmpLib;
using Raven.Server.Documents;
using Raven.Server.Monitoring.OpenTelemetry;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public sealed class DatabaseIndexLockMode : DatabaseIndexScalarObjectBase<OctetString>, ITaggedMetricInstrument<byte>
    {
        public DatabaseIndexLockMode(string databaseName, string indexName, DatabasesLandlord landlord, int databaseIndex, int indexIndex)
            : base(databaseName, indexName, landlord, databaseIndex, indexIndex, SnmpOids.Databases.Indexes.LockMode)
        {
        }

        protected override OctetString GetData(DocumentDatabase database)
        {
            var index = GetIndex(database);
            return new OctetString(index.Definition.LockMode.ToString());
        }

        public Measurement<byte> GetCurrentMeasurement()
        {
            if (TryGetIndex(out var index))
                return new((byte)index.Definition.LockMode, MeasurementTags);

            return default;
        }
        
    }
}

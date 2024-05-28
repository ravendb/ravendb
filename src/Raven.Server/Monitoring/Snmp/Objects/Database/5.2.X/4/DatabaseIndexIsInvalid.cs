using System.Diagnostics.Metrics;
using Lextm.SharpSnmpLib;
using Raven.Server.Documents;
using Raven.Server.Monitoring.OpenTelemetry;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public sealed class DatabaseIndexIsInvalid : DatabaseIndexScalarObjectBase<OctetString>, ITaggedMetricInstrument<byte>
    {
        public DatabaseIndexIsInvalid(string databaseName, string indexName, DatabasesLandlord landlord, int databaseIndex, int indexIndex)
            : base(databaseName, indexName, landlord, databaseIndex, indexIndex, SnmpOids.Databases.Indexes.IsInvalid)
        {
        }

        protected override OctetString GetData(DocumentDatabase database)
        {
            var index = GetIndex(database);
            var stats = index.GetStats();

            return new OctetString(stats.IsInvalidIndex.ToString());
        }

        public Measurement<byte> GetCurrentMeasurement()
        {
            if (TryGetIndex(out var index))
                return new(index.GetStats().IsInvalidIndex ? (byte)1 : (byte)0, MeasurementTags);
            
            return default;
        }
        
    }
}

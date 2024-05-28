using System.Diagnostics.Metrics;
using Lextm.SharpSnmpLib;
using Raven.Server.Documents;
using Raven.Server.Monitoring.OpenTelemetry;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public sealed class DatabaseIndexType : DatabaseIndexScalarObjectBase<OctetString>, ITaggedMetricInstrument<byte>
    {
        public DatabaseIndexType(string databaseName, string indexName, DatabasesLandlord landlord, int databaseIndex, int indexIndex)
            : base(databaseName, indexName, landlord, databaseIndex, indexIndex, SnmpOids.Databases.Indexes.Type)
        {
        }

        protected override OctetString GetData(DocumentDatabase database)
        {
            var index = GetIndex(database);
            return new OctetString(index.Type.ToString());
        }

        public Measurement<byte> GetCurrentMeasurement()
        {
            if (TryGetDatabase(out var database))
                return new((byte)GetIndex(database).Type, MeasurementTags);

            return new(byte.MaxValue, MeasurementTags);
        }
    }
}

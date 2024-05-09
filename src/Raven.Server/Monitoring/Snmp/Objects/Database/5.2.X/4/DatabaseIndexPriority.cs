using System.Diagnostics.Metrics;
using Lextm.SharpSnmpLib;
using Raven.Server.Documents;
using Raven.Server.Monitoring.OpenTelemetry;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public sealed class DatabaseIndexPriority : DatabaseIndexScalarObjectBase<OctetString>, ITaggedMetricInstrument<byte>
    {
        public DatabaseIndexPriority(string databaseName, string indexName, DatabasesLandlord landlord, int databaseIndex, int indexIndex, string nodeTag = null)
            : base(databaseName, indexName, landlord, databaseIndex, indexIndex, SnmpOids.Databases.Indexes.Priority, nodeTag)
        {
        }

        protected override OctetString GetData(DocumentDatabase database)
        {
            var index = GetIndex(database);
            return new OctetString(index.Definition.Priority.ToString());
        }

        public Measurement<byte> GetCurrentValue()
        {
            if (TryGetIndex(out var index))
                return new((byte)index.Definition.Priority, MeasurementTags);

            return default;
        }
    }
}

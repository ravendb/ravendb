using System;
using System.Diagnostics.Metrics;
using System.Globalization;
using Lextm.SharpSnmpLib;
using Raven.Server.Documents;
using Raven.Server.Monitoring.OpenTelemetry;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public sealed class DatabaseIndexExists : DatabaseIndexScalarObjectBase<OctetString>, ITaggedMetricInstrument<byte>
    {
        public DatabaseIndexExists(string databaseName, string indexName, DatabasesLandlord landlord, int databaseIndex, int indexIndex, string nodeTag = null)
            : base(databaseName, indexName, landlord, databaseIndex, indexIndex, SnmpOids.Databases.Indexes.Exists, nodeTag)
        {
        }

        public override ISnmpData Data
        {
            get
            {
                if (Landlord.IsDatabaseLoaded(DatabaseName))
                {
                    var database = Landlord.TryGetOrCreateResourceStore(DatabaseName).Result;
                    var exists = database.IndexStore.GetIndex(IndexName) != null;

                    return new OctetString(exists.ToString(CultureInfo.InvariantCulture));
                }

                return null;
            }
        }

        protected override OctetString GetData(DocumentDatabase database)
        {
            throw new NotSupportedException();
        }

        public Measurement<byte> GetCurrentValue()
        {
            if (TryGetIndex(out var index))
                return new(1, MeasurementTags);

            return new(0, MeasurementTags);
        }
    }
}

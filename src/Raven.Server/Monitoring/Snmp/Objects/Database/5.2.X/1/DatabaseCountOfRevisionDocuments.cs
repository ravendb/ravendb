using System.Collections.Generic;
using System.Diagnostics.Metrics;
using Lextm.SharpSnmpLib;
using Raven.Server.Documents;
using Raven.Server.Monitoring.OpenTelemetry;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public sealed class DatabaseCountOfRevisionDocuments : DatabaseScalarObjectBase<Gauge32>, IMetricInstrument<Measurement<int>>
    {
        private readonly string _databaseName;
        private readonly KeyValuePair<string, object> _measurementTag;

        public DatabaseCountOfRevisionDocuments(string databaseName, DatabasesLandlord landlord, int index)
            : base(databaseName, landlord, SnmpOids.Databases.CountOfRevisionDocuments, index)
        {
            _databaseName = databaseName;
            _measurementTag = new KeyValuePair<string, object>(Monitoring.OpenTelemetry.Constants.Tags.Database, _databaseName);
        }

        protected override Gauge32 GetData(DocumentDatabase database)
        {
            return new Gauge32(GetCount(database));
        }

        private static long GetCount(DocumentDatabase database)
        {
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                return database.DocumentsStorage.RevisionsStorage.GetNumberOfRevisionDocuments(context);
            }
        }

        public Measurement<int> GetCurrentMeasurement()
        {
            return new Measurement<int>(1, _measurementTag);
        }
    }
}

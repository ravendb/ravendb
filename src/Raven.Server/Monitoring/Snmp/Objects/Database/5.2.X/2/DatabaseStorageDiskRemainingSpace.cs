using System;
using System.Diagnostics.Metrics;
using Lextm.SharpSnmpLib;
using Raven.Server.Documents;
using Raven.Server.Monitoring.OpenTelemetry;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Server.Utils;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public sealed class DatabaseStorageDiskRemainingSpace : DatabaseScalarObjectBase<Gauge32>, ITaggedMetricInstrument<long>
    {
        public DatabaseStorageDiskRemainingSpace(string databaseName, DatabasesLandlord landlord, int index, string nodeTag = null)
            : base(databaseName, landlord, SnmpOids.Databases.StorageDiskRemainingSpace, index, nodeTag)
        {
        }

        private long? Value(DocumentDatabase database)
        {
            if (database.Configuration.Core.RunInMemory)
                return null;

            var result = database.MetricCacher.GetValue<DiskSpaceResult>(MetricCacher.Keys.Database.DiskSpaceInfo);
            if (result == null)
                return null;

            return result.TotalFreeSpace.GetValue(SizeUnit.Megabytes);
        }
        
        protected override Gauge32 GetData(DocumentDatabase database)
        {
            if (Value(database) is {} result)
                return new Gauge32(result);

            return null;
        }
        
        public Measurement<long> GetCurrentValue()
        {
            if (TryGetDatabase(out var database) && Value(database) is {} result)
                return new(result, MeasurementTags);
            
            return new(default, MeasurementTags);
        }
    }
}

using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Lextm.SharpSnmpLib;
using Raven.Server.Documents;
using Raven.Server.Monitoring.OpenTelemetry;

namespace Raven.Server.Monitoring.Snmp.Objects
{
    public abstract class DatabaseScalarObjectBase<TData> : ScalarObjectBase<TData>
        where TData : ISnmpData
    {
        protected readonly string DatabaseName;
        protected readonly KeyValuePair<string, object>[] MeasurementTags;
        protected readonly DatabasesLandlord Landlord;

        protected DatabaseScalarObjectBase(string databaseName, DatabasesLandlord landlord, string dots, int index, string nodeTag = null, KeyValuePair<string, object>[] measurementTags = null)
            : base(dots, index)
        {
            DatabaseName = databaseName;
            Landlord = landlord;
            MeasurementTags = new[]
                {
                    new KeyValuePair<string, object>(Constants.Tags.Database, databaseName),
                    new KeyValuePair<string, object>(Constants.Tags.NodeTag, nodeTag)
                }
                .Concat(measurementTags ?? Enumerable.Empty<KeyValuePair<string, object>>())
                .ToArray();
           
            Debug.Assert(MeasurementTags.Select(x => x.Key).Distinct().Count() == MeasurementTags.Count()
               , "MeasurementTags.Select(x => x.Key).Distinct().Count() == MeasurementTags.Count()");     
        }

        protected abstract TData GetData(DocumentDatabase database);

        protected override TData GetData()
        {
            if (Landlord.IsDatabaseLoaded(DatabaseName))
                return GetData(Landlord.TryGetOrCreateResourceStore(DatabaseName).Result);

            return default(TData);
        }

        protected bool TryGetDatabase(out DocumentDatabase database)
        {
            if (Landlord.IsDatabaseLoaded(DatabaseName))
            {
                database = Landlord.TryGetOrCreateResourceStore(DatabaseName).Result;
                return true;
            }

            database = null;
            return false;
        }
        
        
        protected DocumentDatabase GetDatabase()
        {
            if (Landlord.IsDatabaseLoaded(DatabaseName))
                return Landlord.TryGetOrCreateResourceStore(DatabaseName).Result;
            return null;
        }
    }
}

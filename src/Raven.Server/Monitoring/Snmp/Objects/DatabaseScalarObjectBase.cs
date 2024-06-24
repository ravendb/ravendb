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
        protected readonly DatabasesLandlord Landlord;

        protected DatabaseScalarObjectBase(string databaseName, DatabasesLandlord landlord, string dots, int index)
            : base(dots, index)
        {
            DatabaseName = databaseName;
            Landlord = landlord;
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

using Lextm.SharpSnmpLib;
using Raven.Server.Documents;

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
    }
}

using System;
using System.Globalization;
using Lextm.SharpSnmpLib;
using Raven.Server.Documents;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public class DatabaseIndexExists : DatabaseIndexScalarObjectBase<OctetString>
    {
        public DatabaseIndexExists(string databaseName, string indexName, DatabasesLandlord landlord, int databaseIndex, int indexIndex)
            : base(databaseName, indexName, landlord, databaseIndex, indexIndex, "1")
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

                return DefaultValue();
            }
        }

        protected override OctetString GetData(DocumentDatabase database)
        {
            throw new NotSupportedException();
        }
    }
}

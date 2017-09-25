using System;
using System.Globalization;
using Lextm.SharpSnmpLib;
using Raven.Server.Documents;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public class DatabaseLoaded : DatabaseScalarObjectBase<OctetString>
    {
        private readonly string _databaseName;

        public DatabaseLoaded(string databaseName, DatabasesLandlord landlord, int index)
            : base(databaseName, landlord, SnmpOids.Databases.Loaded, index)
        {
            _databaseName = databaseName;
        }

        public override ISnmpData Data => new OctetString(Landlord.IsDatabaseLoaded(_databaseName).ToString(CultureInfo.InvariantCulture));

        protected override OctetString GetData(DocumentDatabase database)
        {
            throw new NotSupportedException();
        }
    }
}

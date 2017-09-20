using Lextm.SharpSnmpLib;
using Raven.Server.Documents;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public class DatabaseName : DatabaseScalarObjectBase<OctetString>
    {
        private readonly OctetString _name;

        public DatabaseName(string databaseName, DatabasesLandlord landlord, int index)
            : base(databaseName, landlord, "5.2.{0}.1.1", index)
        {
            _name = new OctetString(databaseName);
        }

        protected override OctetString GetData(DocumentDatabase database)
        {
            return _name;
        }
    }
}

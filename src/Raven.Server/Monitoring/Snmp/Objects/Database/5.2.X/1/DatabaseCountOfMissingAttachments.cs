using Lextm.SharpSnmpLib;
using Raven.Server.Documents;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public class DatabaseCountOfMissingAttachments : DatabaseScalarObjectBase<Gauge32>
    {
        public DatabaseCountOfMissingAttachments(string databaseName, DatabasesLandlord landlord, int index)
            : base(databaseName, landlord, SnmpOids.Databases.CountOfMissingAttachments, index)
        {
        }

        protected override Gauge32 GetData(DocumentDatabase database)
        {
            return new Gauge32(GetCount(database));
        }

        private static long GetCount(DocumentDatabase database)
        {
            return database.DocumentsStorage.AttachmentsStorage.GetMissingAttachmentsCount();
        }
    }
}

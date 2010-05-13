using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Database.Plugins;

namespace Raven.Bundles.Replication
{
    public class RemoveConflictOnPutTrigger : IPutTrigger, IRequiresDocumentDatabaseInitialization
    {
        public const string RavenReplicationConflict = "Raven-Replication-Conflict";
        private DocumentDatabase docDb;

        public VetoResult AllowPut(string key, JObject document, JObject metadata, TransactionInformation transactionInformation)
        {
            return VetoResult.Allowed;
        }

        public void OnPut(string key, JObject document, JObject metadata, TransactionInformation transactionInformation)
        {
            var oldVersion = docDb.Get(key, transactionInformation);
            if (oldVersion == null)
                return;
            if (oldVersion.Metadata[RavenReplicationConflict] == null)
                return;
            // this is a conflict document, holding document keys in the 
            // values of the properties
            foreach (JProperty prop in oldVersion.DataAsJson)
            {
                docDb.Delete(prop.Value<string>(), null, transactionInformation);
            }
        }

        public void AfterCommit(string key, JObject document, JObject metadata)
        {
        }

        public void Initialize(DocumentDatabase database)
        {
            docDb = database;
        }
    }
}
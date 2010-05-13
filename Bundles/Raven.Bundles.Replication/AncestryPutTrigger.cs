using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Database.Plugins;

namespace Raven.Bundles.Replication
{
    public class AncestryPutTrigger : IPutTrigger, IRequiresDocumentDatabaseInitialization
    {
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
            var ancestry = metadata.Value<JArray>(ReplicationConstants.RavenAncestry);
            if(ancestry == null)
            {
                ancestry = new JArray();
                metadata.Add(ReplicationConstants.RavenAncestry, ancestry);
            }
            ancestry.Add(JToken.FromObject(oldVersion.Etag.ToString()));
            if(ancestry.Count > 15)
                ancestry.RemoveAt(0);
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
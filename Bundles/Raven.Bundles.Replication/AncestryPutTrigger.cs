using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Database.Plugins;

namespace Raven.Bundles.Replication
{
    public class AncestryPutTrigger : AbstractPutTrigger
    {
        public override void OnPut(string key, JObject document, JObject metadata, TransactionInformation transactionInformation)
        {
            var oldVersion = Database.Get(key, transactionInformation);
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
    }
}
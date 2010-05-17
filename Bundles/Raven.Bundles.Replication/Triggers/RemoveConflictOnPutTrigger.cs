using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Database.Plugins;

namespace Raven.Bundles.Replication.Triggers
{
    public class RemoveConflictOnPutTrigger : AbstractPutTrigger
    {
        public override void OnPut(string key, JObject document, JObject metadata, TransactionInformation transactionInformation)
        {
           using(ReplicationContext.Enter())
           {
               var oldVersion = Database.Get(key, transactionInformation);
               if (oldVersion == null)
                   return;
               if (oldVersion.Metadata[ReplicationConstants.RavenReplicationConflict] == null)
                   return;
               // this is a conflict document, holding document keys in the 
               // values of the properties
               foreach (var prop in oldVersion.DataAsJson)
               {
                   Database.Delete(prop.Value.Value<string>(), null, transactionInformation);
               }
           }
        }
    }
}
using System;
using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Database.Plugins;

namespace Raven.Bundles.Replication
{
    /// <summary>
    /// We can't allow real deletes when using replication, because
    /// then we won't have any way to replicate the delete. Instead
    /// we allow the delete but don't do actual delete, we replace it 
    /// with a delete marker instead
    /// </summary>
    public class VirtualDeleteTrigger : AbstractDeleteTrigger
    {
        public override void AfterDelete(string key, TransactionInformation transactionInformation)
        {
            Database.Put(key, null, new JObject(), new JObject(new JProperty("Raven-Delete-Marker", true)),
                         transactionInformation);
        }
    }
}
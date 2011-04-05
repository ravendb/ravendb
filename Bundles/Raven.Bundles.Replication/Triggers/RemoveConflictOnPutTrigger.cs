//-----------------------------------------------------------------------
// <copyright file="RemoveConflictOnPutTrigger.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.ComponentModel.Composition;
using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Database.Plugins;
using Raven.Http;
using Raven.Json.Linq;

namespace Raven.Bundles.Replication.Triggers
{
	[ExportMetadata("Order", 10000)]
	public class RemoveConflictOnPutTrigger : AbstractPutTrigger
    {
		public override void OnPut(string key, RavenJObject document, RavenJObject metadata, TransactionInformation transactionInformation)
        {
           using(Database.DisableAllTriggersForCurrentThread())
           {
               metadata.Remove(ReplicationConstants.RavenReplicationConflict);// you can't put conflicts

               var oldVersion = Database.Get(key, transactionInformation);
               if (oldVersion == null)
                   return;
               if (oldVersion.Metadata[ReplicationConstants.RavenReplicationConflict] == null)
                   return;
               // this is a conflict document, holding document keys in the 
               // values of the properties
               foreach (var prop in oldVersion.DataAsJson.Value<JArray>("Conflicts"))
               {
                   Database.Delete(prop.Value<string>(), null, transactionInformation);
               }
           }
        }
    }
}

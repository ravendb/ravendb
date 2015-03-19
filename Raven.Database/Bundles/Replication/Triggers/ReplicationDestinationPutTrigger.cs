using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using Newtonsoft.Json;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Replication;
using Raven.Database.Plugins;
using Raven.Json.Linq;

namespace Raven.Database.Bundles.Replication.Triggers
{
	[ExportMetadata("Bundle", "Replication")]
	[ExportMetadata("Order", 10000)]
	[InheritedExport(typeof (AbstractPutTrigger))]
	public class ReplicationDestinationPutTrigger : AbstractPutTrigger
	{
		public override void AfterPut(string key, RavenJObject document, RavenJObject metadata, Etag etag, TransactionInformation transactionInformation)
		{		
//			Database.Patches.ApplyPatch(key, null, new ScriptedPatchRequest
//			{
//				Script = @"
//							_.forEach(this.Destinations, function(destination){
//								if(typeof destination.SourceCollections !== 'undefined' && destination.SourceCollections.length > 0)
//									this.IgnoredClient = true;
//								else
//									this.IgnoredClient = false;
//							})
//						  "
//			}, transactionInformation);
		}
	}
}

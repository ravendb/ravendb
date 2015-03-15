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

		private readonly ILog log = LogManager.GetCurrentClassLogger();

		public override void AfterPut(string key, RavenJObject document, RavenJObject metadata, Etag etag, TransactionInformation transactionInformation)
		{
//			ReplicationDocument replicationDestination;
//			try
//			{
//				replicationDestination = document.JsonDeserialization<ReplicationDocument>();
//			}
//			catch (InvalidOperationException e)
//			{
//				log.Error("Failed to deserialize replication destination key. This should not happen, this is probably a bug. Exception thrown: " + e);
//				return;
//			}
//			catch (InvalidDataException e)
//			{
//				log.Error("Failed to deserialize replication destination key. This should not happen, this is probably a bug. Exception thrown: " + e);
//				return;
//			}
//
//			var shouldUpdate = false;
//			foreach (var dest in replicationDestination.Destinations
//													   .Where(dest => dest.ShouldReplicateFromSpecificCollections))
//			{
//				dest.IgnoredClient = true;
//				shouldUpdate = true;
//			}
//
//			if (shouldUpdate)
//				Database.Documents.Put(key, null, RavenJObject.FromObject(replicationDestination), metadata, transactionInformation);			
		}
	}
}

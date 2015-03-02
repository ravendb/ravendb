using System;
using System.ComponentModel.Composition;
using System.Linq;
using System.Threading;
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
		private readonly static ILog log = LogManager.GetCurrentClassLogger();
		private const int TouchBatchSize = 1024;

		public override void OnPut(string key, RavenJObject document, RavenJObject metadata, TransactionInformation transactionInformation)
		{
			//TODO: revise this --> probably needs refactoring to address multiple replication document PUTs in short period of time
			if (!key.Equals(Constants.RavenReplicationDestinations, StringComparison.InvariantCultureIgnoreCase) || 
				document == null)
				return;

			ReplicationDocument replicationDocument;
			try
			{
				replicationDocument = document.JsonDeserialization<ReplicationDocument>();
			}
			catch (Exception e)
			{
				log.ErrorException("Failed to deserialize replication document. Please check if a document with key '" + Constants.RavenReplicationDestinations + "' is correct.", e);
				return;
			}

			foreach (var destination in replicationDocument.Destinations.Where(dest => dest.ShouldReplicateFromSpecificCollections &&
			                                                                           dest.SourceCollections != null &&
			                                                                           dest.SourceCollections.Length > 0))
			{
				foreach (var collectionName in destination.SourceCollections)
				{
					bool stale;
					var docIds = Database.Queries.QueryDocumentIds(Constants.DocumentsByEntityNameIndex, new IndexQuery
					{
						Query = "Tag:" + collectionName,
						WaitForNonStaleResultsAsOfNow = true
					}, new CancellationTokenSource(), out stale).ToArray();

					for (int index = 0; index < docIds.Length; index += TouchBatchSize)
					{
						var currentIdBatch = docIds.Skip(index).Take(TouchBatchSize);
						Database.TransactionalStorage.Batch(accessor =>
						{
							foreach (var id in currentIdBatch)
							{
								Etag preTouchEtag;
								Etag afterTouchEtag;
								accessor.Documents.TouchDocument(id, out preTouchEtag, out afterTouchEtag);
							}
						});
						
					}

				}
			}
		}
	}
}

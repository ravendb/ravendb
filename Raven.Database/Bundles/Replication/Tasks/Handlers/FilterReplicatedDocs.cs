// -----------------------------------------------------------------------
//  <copyright file="FilteringHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Bundles.Replication.Tasks;
using Raven.Database.Actions;
using Raven.Database.Prefetching;

namespace Raven.Database.Bundles.Replication.Tasks.Handlers
{
	public class FilterReplicatedDocs : IReplicatedDocsHandler
	{
		private readonly static ILog Log = LogManager.GetCurrentClassLogger();

		private readonly DocumentActions docActions;
		private readonly ReplicationStrategy strategy;
		private readonly PrefetchingBehavior prefetchingBehavior;
		private readonly string destinationId;
		private readonly Etag lastEtag;

		public FilterReplicatedDocs(DocumentActions docActions, ReplicationStrategy strategy, PrefetchingBehavior prefetchingBehavior, string destinationId, Etag lastEtag)
		{
			this.docActions = docActions;
			this.strategy = strategy;
			this.prefetchingBehavior = prefetchingBehavior;
			this.destinationId = destinationId;
			this.lastEtag = lastEtag;
		}

		public IEnumerable<JsonDocument> Handle(IEnumerable<JsonDocument> docs)
		{
			return docs
				.Where(document =>
				{
					var info = docActions.GetRecentTouchesFor(document.Key);
					if (info != null)
					{
						if (info.TouchedEtag.CompareTo(lastEtag) > 0)
						{
							Log.Debug(
								"Will not replicate document '{0}' to '{1}' because the updates after etag {2} are related document touches",
								document.Key, destinationId, info.TouchedEtag);
							return false;
						}
					}

					string reason;
					return strategy.FilterDocuments(destinationId, document.Key, document.Metadata, out reason) &&
					       prefetchingBehavior.FilterDocuments(document);
				});
		}
	}
}
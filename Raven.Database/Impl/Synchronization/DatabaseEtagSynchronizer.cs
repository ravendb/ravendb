// -----------------------------------------------------------------------
//  <copyright file="DatabaseEtagSynchronizer.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Database.Storage;

namespace Raven.Database.Impl.Synchronization
{
	public class DatabaseEtagSynchronizer
	{
		private readonly IDictionary<EtagSynchronizerType, EtagSynchronizer> etagSynchronizers;

		public DatabaseEtagSynchronizer(ITransactionalStorage transactionalStorage)
		{
			etagSynchronizers = new Dictionary<EtagSynchronizerType, EtagSynchronizer>
			{
				{EtagSynchronizerType.Indexer, new EtagSynchronizer(EtagSynchronizerType.Indexer, transactionalStorage)},
				{EtagSynchronizerType.Reducer, new EtagSynchronizer(EtagSynchronizerType.Reducer, transactionalStorage)},
				{EtagSynchronizerType.Replicator, new EtagSynchronizer(EtagSynchronizerType.Replicator, transactionalStorage)},
				{EtagSynchronizerType.SqlReplicator, new EtagSynchronizer(EtagSynchronizerType.SqlReplicator, transactionalStorage)}
			};
		}

		public EtagSynchronizer GetSynchronizer(EtagSynchronizerType type)
		{
			return etagSynchronizers[type];
		}

		public void UpdateSynchronizationState(JsonDocument[] docs)
		{
			if (docs == null)
				return;

			var lowestEtag = GetLowestEtag(docs);

			foreach (var key in etagSynchronizers.Keys)
			{
				etagSynchronizers[key].UpdateSynchronizationState(lowestEtag);
			}
		}

		private static Etag GetLowestEtag(IList<JsonDocument> documents)
		{
			if (documents == null || documents.Count == 0)
				return Etag.Empty;

			var lowest = documents[0].Etag;

			for (var i = 1; i < documents.Count; i++)
			{
				var etag = documents[i].Etag;
				if (lowest.CompareTo(etag) <= 0)
					continue;

				lowest = etag;
			}

			return lowest;
		}
	}
}
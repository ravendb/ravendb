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
		private readonly ITransactionalStorage transactionalStorage;
		private IDictionary<EtagSynchronizerType, EtagSynchronizer> etagSynchronizers = new Dictionary<EtagSynchronizerType, EtagSynchronizer>();

		public DatabaseEtagSynchronizer(ITransactionalStorage transactionalStorage)
		{
			this.transactionalStorage = transactionalStorage;
		}

		public EtagSynchronizer GetSynchronizer(EtagSynchronizerType type)
		{
			EtagSynchronizer value;
			if (etagSynchronizers.TryGetValue(type, out value))
				return value;
			lock (this)
			{
				if (etagSynchronizers.TryGetValue(type, out value))
					return value;
		
				value = new EtagSynchronizer(type, transactionalStorage);
				etagSynchronizers = new Dictionary<EtagSynchronizerType, EtagSynchronizer>(etagSynchronizers)
				{
					{type, value}
				};
				return value;
			}
		}

		public void UpdateSynchronizationState(JsonDocument[] docs)
		{
			if (docs == null)
				return;

			var lowestEtag = GetLowestEtag(docs);

			foreach (var key in etagSynchronizers)
			{
				key.Value.UpdateSynchronizationState(lowestEtag);
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
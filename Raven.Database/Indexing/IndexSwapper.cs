// -----------------------------------------------------------------------
//  <copyright file="IndexSwapper.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Util;
using Raven.Client.Connection;
using Raven.Json.Linq;

namespace Raven.Database.Indexing
{
	public class IndexSwapper
	{
		public DocumentDatabase Database { get; set; }

		private readonly ConcurrentDictionary<int, IndexSwapInformation> indexesToSwap = new ConcurrentDictionary<int, IndexSwapInformation>();

		private const string IndexSwapPrefix = "Raven/Indexes/Swap/";

		public IndexSwapper(DocumentDatabase database)
		{
			Database = database;

			database.Notifications.OnDocumentChange += (db, notification, metadata) =>
			{
				if (notification.Id == null)
					return;

				if (notification.Id.StartsWith(IndexSwapPrefix, StringComparison.OrdinalIgnoreCase) == false)
					return;

				var swapIndexName = notification.Id.Substring(IndexSwapPrefix.Length);

				if (notification.Type == DocumentChangeTypes.Delete)
				{
					HandleIndexSwapDocumentDelete(swapIndexName);
					return;
				}

				var document = db.Documents.Get(notification.Id, null);
				HandleIndexSwapDocument(document);
			};

			Initialize();
		}

		private void Initialize()
		{
			int nextStart = 0;
			var documents = Database.Documents.GetDocumentsWithIdStartingWith(IndexSwapPrefix, null, null, 0, int.MaxValue, Database.WorkContext.CancellationToken, ref nextStart);

			foreach (RavenJObject document in documents)
			{
				HandleIndexSwapDocument(document.ToJsonDocument());
			}
		}

		private void HandleIndexSwapDocument(JsonDocument document)
		{
			if (document == null)
				return;

			var id = document.Key;
			var swapIndexName = id.Substring(IndexSwapPrefix.Length);

			var swapIndex = Database.IndexStorage.GetIndexInstance(swapIndexName);
			if (swapIndex == null)
			{
				DeleteIndexSwapDocument(id);
				return;
			}

			var swapIndexId = swapIndex.IndexId;

			var indexSwapInformation = document.DataAsJson.JsonDeserialization<IndexSwapInformation>();
			indexSwapInformation.SwapIndex = swapIndexName;

			if (string.Equals(swapIndexName, indexSwapInformation.IndexToReplace, StringComparison.OrdinalIgnoreCase))
			{
				DeleteIndexSwapDocument(id);
				return;
			}

			if (indexSwapInformation.SwapTimeUtc.HasValue)
				indexSwapInformation.SwapTimer = Database.TimerManager.NewTimer(state => SwapIndexes(new Dictionary<int, IndexSwapInformation> { { swapIndexId, indexSwapInformation } }), SystemTime.UtcNow - indexSwapInformation.SwapTimeUtc.Value, TimeSpan.FromDays(7));

			indexesToSwap.AddOrUpdate(swapIndexId, s => indexSwapInformation, (s, old) =>
			{
				if (old.SwapTimer != null)
					Database.TimerManager.ReleaseTimer(old.SwapTimer);

				return indexSwapInformation;
			});
		}

		private void DeleteIndexSwapDocument(string documentKey)
		{
			Database.Documents.Delete(documentKey, null, null);
		}

		private void HandleIndexSwapDocumentDelete(string swapIndexName)
		{
			var pair = indexesToSwap.FirstOrDefault(x => string.Equals(x.Value.SwapIndex, swapIndexName, StringComparison.OrdinalIgnoreCase));
			IndexSwapInformation indexSwapInformation;
			if (indexesToSwap.TryRemove(pair.Key, out indexSwapInformation) && indexSwapInformation.SwapTimer != null)
				Database.TimerManager.ReleaseTimer(indexSwapInformation.SwapTimer);
		}

		public void SwapIndexes(ICollection<int> indexIds)
		{
			if (indexIds.Count == 0 || indexesToSwap.Count == 0)
				return;

			var indexes = new Dictionary<int, IndexSwapInformation>();

			foreach (var indexId in indexIds)
			{
				IndexSwapInformation indexSwapInformation;
				if (indexesToSwap.TryGetValue(indexId, out indexSwapInformation) == false)
					continue;

				var shouldSwap = false;
				Database.TransactionalStorage.Batch(accessor =>
				{
					if (Database.IndexStorage.IsIndexStale(indexId, Database.LastCollectionEtags) == false)
						shouldSwap = true; // always swap non-stale indexes
					else
					{
						var swapIndex = Database.IndexStorage.GetIndexInstance(indexId);

						var statistics = accessor.Indexing.GetIndexStats(indexId);
						if (swapIndex.IsMapReduce)
						{
							if (EtagUtil.IsGreaterThanOrEqual(statistics.LastReducedEtag, indexSwapInformation.MinimumSwapEtag))
							{
								shouldSwap = true;
							}
						}
						else
						{
							if (EtagUtil.IsGreaterThanOrEqual(statistics.LastIndexedEtag, indexSwapInformation.MinimumSwapEtag))
							{
								shouldSwap = true;
							}
						}
					}
				});

				if (shouldSwap)
					indexes.Add(indexId, indexSwapInformation);
			}

			SwapIndexes(indexes);
		}

		private void SwapIndexes(Dictionary<int, IndexSwapInformation> indexes)
		{
			if (indexes.Count == 0)
				return;

			using (Database.IndexDefinitionStorage.TryRemoveIndexContext())
			{
				foreach (var pair in indexes)
				{
					var indexSwapInformation = pair.Value;

					if (Database.IndexStorage.SwapIndex(indexSwapInformation.SwapIndex, indexSwapInformation.IndexToReplace))
						Database.Documents.Delete(IndexSwapPrefix + indexSwapInformation.SwapIndex, null, null);
				}
			}
		}

		private class IndexSwapInformation : IndexSwapDocument
		{
			public Timer SwapTimer { get; set; }

			public string SwapIndex { get; set; }
		}
	}

	public class IndexSwapDocument
	{
		public string IndexToReplace { get; set; }

		public Etag MinimumSwapEtag { get; set; }

		public DateTime? SwapTimeUtc { get; set; }
	}
}
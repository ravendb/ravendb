// -----------------------------------------------------------------------
//  <copyright file="IndexReplacer.cs" company="Hibernating Rhinos LTD">
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
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
using Raven.Client.Connection;
using Raven.Database.Extensions;
using Raven.Json.Linq;

namespace Raven.Database.Indexing
{
	public class IndexReplacer
	{
		private readonly ILog log = LogManager.GetCurrentClassLogger();

		public DocumentDatabase Database { get; set; }

		private readonly ConcurrentDictionary<int, IndexReplaceInformation> indexesToReplace = new ConcurrentDictionary<int, IndexReplaceInformation>();

		public IndexReplacer(DocumentDatabase database)
		{
			Database = database;

			database.Notifications.OnDocumentChange += (db, notification, metadata) =>
			{
				if (notification.Id == null)
					return;

				if (notification.Id.StartsWith(Constants.IndexReplacePrefix, StringComparison.OrdinalIgnoreCase) == false)
					return;

				var replaceIndexName = notification.Id.Substring(Constants.IndexReplacePrefix.Length);

				if (notification.Type == DocumentChangeTypes.Delete)
				{
					HandleIndexReplaceDocumentDelete(replaceIndexName);
					return;
				}

				var document = db.Documents.Get(notification.Id, null);
				var replaceIndexId = HandleIndexReplaceDocument(document);

				if (replaceIndexId != null)
					ReplaceIndexes(new []{ replaceIndexId.Value });
			};

			Initialize();
		}

		private void Initialize()
		{
			int nextStart = 0;
			var documents = Database.Documents.GetDocumentsWithIdStartingWith(Constants.IndexReplacePrefix, null, null, 0, int.MaxValue, Database.WorkContext.CancellationToken, ref nextStart);

			var indexes = new List<int>();
			foreach (RavenJObject document in documents)
			{
				var replaceIndexId = HandleIndexReplaceDocument(document.ToJsonDocument());
				if (replaceIndexId.HasValue)
					indexes.Add(replaceIndexId.Value);
			}

			ReplaceIndexes(indexes);
		}

		private int? HandleIndexReplaceDocument(JsonDocument document)
		{
			if (document == null)
				return null;

			var id = document.Key;
			var replaceIndexName = id.Substring(Constants.IndexReplacePrefix.Length);

			var replaceIndex = Database.IndexStorage.GetIndexInstance(replaceIndexName);
			if (replaceIndex == null)
			{
				DeleteIndexReplaceDocument(id);
				return null;
			}

			var replaceIndexId = replaceIndex.IndexId;

		    var indexDefinition = Database.IndexDefinitionStorage.GetIndexDefinition(replaceIndexId);
            if (!indexDefinition.IsSideBySideIndex)
            {
                indexDefinition.IsSideBySideIndex = true;
                Database.IndexDefinitionStorage.UpdateIndexDefinitionWithoutUpdatingCompiledIndex(indexDefinition);
            }

			var indexReplaceInformation = document.DataAsJson.JsonDeserialization<IndexReplaceInformation>();
			indexReplaceInformation.ReplaceIndex = replaceIndexName;

			if (string.Equals(replaceIndexName, indexReplaceInformation.IndexToReplace, StringComparison.OrdinalIgnoreCase))
			{
				DeleteIndexReplaceDocument(id);
				return null;
			}

			if (indexReplaceInformation.ReplaceTimeUtc.HasValue)
			{
				var dueTime = indexReplaceInformation.ReplaceTimeUtc.Value - SystemTime.UtcNow;
				if (dueTime.TotalSeconds < 0) 
					dueTime = TimeSpan.Zero;

				indexReplaceInformation.ReplaceTimer = Database.TimerManager.NewTimer(state => ReplaceIndexes(new Dictionary<int, IndexReplaceInformation> { { replaceIndexId, indexReplaceInformation } }), dueTime, TimeSpan.FromDays(7));
			}

			indexesToReplace.AddOrUpdate(replaceIndexId, s => indexReplaceInformation, (s, old) =>
			{
				if (old.ReplaceTimer != null)
					Database.TimerManager.ReleaseTimer(old.ReplaceTimer);

				return indexReplaceInformation;
			});

			return replaceIndexId;
		}

		private void DeleteIndexReplaceDocument(string documentKey)
		{
			Database.Documents.Delete(documentKey, null, null);
		}

		private void HandleIndexReplaceDocumentDelete(string replaceIndexName)
		{
			var pair = indexesToReplace.FirstOrDefault(x => string.Equals(x.Value.ReplaceIndex, replaceIndexName, StringComparison.OrdinalIgnoreCase));
			IndexReplaceInformation indexReplaceInformation;
			if (indexesToReplace.TryRemove(pair.Key, out indexReplaceInformation) && indexReplaceInformation.ReplaceTimer != null)
				Database.TimerManager.ReleaseTimer(indexReplaceInformation.ReplaceTimer);

            Database.Indexes.DeleteIndex(replaceIndexName);
		}

		public void ReplaceIndexes(ICollection<int> indexIds)
		{
			if (indexIds.Count == 0 || indexesToReplace.Count == 0)
				return;

			var indexes = new Dictionary<int, IndexReplaceInformation>();

			foreach (var indexId in indexIds)
			{
				IndexReplaceInformation indexReplaceInformation;
				if (indexesToReplace.TryGetValue(indexId, out indexReplaceInformation) == false)
					continue;

				var shouldReplace = false;
				Database.TransactionalStorage.Batch(accessor =>
				{
					if (indexReplaceInformation.Forced 
						|| Database.IndexStorage.IsIndexStale(indexId, Database.LastCollectionEtags) == false)
						shouldReplace = true; // always replace non-stale or forced indexes
					else
					{
						var replaceIndex = Database.IndexStorage.GetIndexInstance(indexId);

						var statistics = accessor.Indexing.GetIndexStats(indexId);
						if (replaceIndex.IsMapReduce)
						{
							if (statistics.LastReducedEtag != null && EtagUtil.IsGreaterThanOrEqual(statistics.LastReducedEtag, indexReplaceInformation.MinimumEtagBeforeReplace))
								shouldReplace = true;
						}
						else
						{
							if (statistics.LastIndexedEtag != null && EtagUtil.IsGreaterThanOrEqual(statistics.LastIndexedEtag, indexReplaceInformation.MinimumEtagBeforeReplace))
								shouldReplace = true;
						}

						if (shouldReplace == false && indexReplaceInformation.ReplaceTimeUtc.HasValue && (indexReplaceInformation.ReplaceTimeUtc.Value - SystemTime.UtcNow).TotalSeconds < 0) 
							shouldReplace = true;
					}
				});

				if (shouldReplace)
					indexes.Add(indexId, indexReplaceInformation);
			}

			ReplaceIndexes(indexes);
		}

		public void ForceReplacement(IndexDefinition indexDefiniton)
		{
			var indexId = indexDefiniton.IndexId;
			IndexReplaceInformation indexReplaceInformation;
			if (indexesToReplace.TryGetValue(indexId, out indexReplaceInformation) == false)
				return;

			indexReplaceInformation.Forced = true;

			ReplaceIndexes(new List<int> { indexId });
		}

		private void ReplaceIndexes(Dictionary<int, IndexReplaceInformation> indexes)
		{
			if (indexes.Count == 0)
				return;

			try
			{
				using (Database.IndexDefinitionStorage.TryRemoveIndexContext())
				{
					foreach (var pair in indexes)
					{
						var indexReplaceInformation = pair.Value;

						try
						{
							if (Database.IndexStorage.ReplaceIndex(indexReplaceInformation.ReplaceIndex, indexReplaceInformation.IndexToReplace))
								Database.Documents.Delete(Constants.IndexReplacePrefix + indexReplaceInformation.ReplaceIndex, null, null);
							else
							{
								indexReplaceInformation.ErrorCount++;

								if (indexReplaceInformation.ReplaceTimer != null)
								{
									if (indexReplaceInformation.ErrorCount <= 10) 
										indexReplaceInformation.ReplaceTimer.Change(TimeSpan.FromMinutes(1), TimeSpan.FromDays(7)); // try again in one minute
									else
									{
										Database.TimerManager.ReleaseTimer(indexReplaceInformation.ReplaceTimer);
										indexReplaceInformation.ReplaceTimer = null;
									}
								}

								var message = string.Format("Index replace failed. Could not replace index '{0}' with '{1}'.", indexReplaceInformation.IndexToReplace, indexReplaceInformation.ReplaceIndex);

								Database.AddAlert(new Alert
								{
									AlertLevel = AlertLevel.Error,
									CreatedAt = SystemTime.UtcNow,
									Message = message,
									Title = "Index replace failed",
									UniqueKey = string.Format("Index '{0}' errored, dbid: {1}", indexReplaceInformation.ReplaceIndex, Database.TransactionalStorage.Id),
								});

								log.Error(message);
							}
						}
						catch (Exception e)
						{
							var message = string.Format("Index replace failed. Could not replace index '{0}' with '{1}'", indexReplaceInformation.IndexToReplace, indexReplaceInformation.ReplaceIndex);

							log.ErrorException(message, e);

							indexReplaceInformation.ErrorCount++;
						}
						
					}
				}
			}
			catch (InvalidOperationException)
			{
				// could not get lock, ignore?
			}
		}

		private class IndexReplaceInformation : IndexReplaceDocument
		{
			public Timer ReplaceTimer { get; set; }

			public string ReplaceIndex { get; set; }

			public int ErrorCount { get; set; }

			public bool Forced { get; set; }
		}
	}
}
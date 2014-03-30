// -----------------------------------------------------------------------
//  <copyright file="IndexingStorageActions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Util.Streams;
using Raven.Database.Util.Streams;

namespace Raven.Database.Storage.Voron.StorageActions
{
	using System;
	using System.Collections.Generic;
	using System.Diagnostics;
	using System.Linq;
	using System.Threading;

	using Raven.Abstractions;
	using Raven.Abstractions.Data;
	using Raven.Abstractions.Exceptions;
	using Raven.Abstractions.Extensions;
	using Raven.Database.Impl;
	using Raven.Database.Indexing;
	using Raven.Database.Storage.Voron.Impl;
	using Raven.Json.Linq;

	using global::Voron;
	using global::Voron.Impl;

	public class IndexingStorageActions : StorageActionsBase, IIndexingStorageActions
	{
		private readonly TableStorage tableStorage;

		private readonly Reference<WriteBatch> writeBatch;

		private readonly IUuidGenerator generator;

		private readonly IStorageActionsAccessor currentStorageActionsAccessor;

        public IndexingStorageActions(TableStorage tableStorage, IUuidGenerator generator, Reference<SnapshotReader> snapshot, Reference<WriteBatch> writeBatch, IStorageActionsAccessor storageActionsAccessor, IBufferPool bufferPool)
			: base(snapshot, bufferPool)
		{
			this.tableStorage = tableStorage;
			this.generator = generator;
			this.writeBatch = writeBatch;
			this.currentStorageActionsAccessor = storageActionsAccessor;
		}

		public void Dispose()
		{
		}

		public IEnumerable<IndexStats> GetIndexesStats()
		{
			using (var indexingStatsIterator = tableStorage.IndexingStats.Iterate(Snapshot, writeBatch.Value))
			{
				if (!indexingStatsIterator.Seek(Slice.BeforeAllKeys))
					yield break;

				do
				{
					var key = indexingStatsIterator.CurrentKey.ToString();

					RavenJObject indexStats;
					using (var stream = indexingStatsIterator.CreateReaderForCurrent().AsStream())
					{
						indexStats = stream.ToJObject();
					}

					ushort version;
					var reduceStats = Load(tableStorage.ReduceStats, key, out version);
					var lastIndexedEtag = Load(tableStorage.LastIndexedEtags, key, out version);

					yield return GetIndexStats(indexStats, reduceStats, lastIndexedEtag);
				}
				while (indexingStatsIterator.MoveNext());
			}
		}

		public IndexStats GetIndexStats(int id)
		{
			var key = CreateKey(id);

			ushort version;

			var indexStats = Load(tableStorage.IndexingStats, key, out version);
			var reduceStats = Load(tableStorage.ReduceStats, key, out version);
			var lastIndexedEtags = Load(tableStorage.LastIndexedEtags, key, out version);

			return GetIndexStats(indexStats, reduceStats, lastIndexedEtags);
		}

		public void AddIndex(int id, bool createMapReduce)
		{
			var key = CreateKey(id);

			if (tableStorage.IndexingStats.Contains(Snapshot, key, writeBatch.Value))
				throw new ArgumentException(string.Format("There is already an index with the name: '{0}'", id));

			tableStorage.IndexingStats.Add(
				writeBatch.Value,
				key,
				new RavenJObject
				{
					{ "index", id },
					{ "attempts", 0 },
					{ "successes", 0 },
					{ "failures", 0 },
					{ "priority", 1 },
					{ "touches", 0 },
					{ "createdTimestamp", SystemTime.UtcNow },
					{ "lastIndexingTime", DateTime.MinValue }
				}, 0);

			tableStorage.ReduceStats.Add(
				writeBatch.Value,
				key,
				new RavenJObject
				{
					{ "reduce_attempts", createMapReduce ? 0 : (RavenJToken)RavenJValue.Null },
					{ "reduce_successes", createMapReduce ? 0 : (RavenJToken)RavenJValue.Null },
					{ "reduce_failures", createMapReduce ? 0 : (RavenJToken)RavenJValue.Null },
					{ "lastReducedEtag", createMapReduce ? Etag.Empty.ToByteArray() : (RavenJToken)RavenJValue.Null },
					{ "lastReducedTimestamp", createMapReduce ? DateTime.MinValue : (RavenJToken)RavenJValue.Null }
				}, 0);

			tableStorage.LastIndexedEtags.Add(
				writeBatch.Value,
				key,
				new RavenJObject
				{
					{ "index", id },
					{ "lastEtag", Etag.Empty.ToByteArray() },
					{ "lastTimestamp", DateTime.MinValue },
				}, 0);
		}

		public void PrepareIndexForDeletion(int id)
		{
			var key = CreateKey(id);

			tableStorage.IndexingStats.Delete(writeBatch.Value, key);
			tableStorage.ReduceStats.Delete(writeBatch.Value, key);
			tableStorage.LastIndexedEtags.Delete(writeBatch.Value, key);
		}

		public void DeleteIndex(int id, CancellationToken token)
		{
			token.ThrowIfCancellationRequested();

			RemoveAllDocumentReferencesByView(id);

			var mappedResultsStorageActions = (MappedResultsStorageActions)currentStorageActionsAccessor.MapReduce;

			mappedResultsStorageActions.DeleteMappedResultsForView(id);
			mappedResultsStorageActions.DeleteScheduledReductionForView(id);
			mappedResultsStorageActions.RemoveReduceResultsForView(id);
		}

		public void SetIndexPriority(int id, IndexingPriority priority)
		{
			var key = CreateKey(id);

			ushort version;
			var index = Load(tableStorage.IndexingStats, key, out version);

			index["priority"] = (int)priority;

			tableStorage.IndexingStats.Add(writeBatch.Value, key, index, version);
		}

		public IndexFailureInformation GetFailureRate(int id)
		{
			var key = CreateKey(id);

			ushort version;
			var indexStats = Load(tableStorage.IndexingStats, key, out version);
			var reduceStats = Load(tableStorage.ReduceStats, key, out version);

			var indexFailureInformation = new IndexFailureInformation
			{
				Attempts = indexStats.Value<int>("attempts"),
				Errors = indexStats.Value<int>("failures"),
				Successes = indexStats.Value<int>("successes"),
				ReduceAttempts = reduceStats.Value<int?>("reduce_attempts"),
				ReduceErrors = reduceStats.Value<int?>("reduce_failures"),
				ReduceSuccesses = reduceStats.Value<int?>("reduce_successes"),
				Id = indexStats.Value<int>("index")
			};

			return indexFailureInformation;
		}

		public void UpdateLastIndexed(int id, Etag etag, DateTime timestamp)
		{
			var key = CreateKey(id);

			ushort version;
			var indexStats = Load(tableStorage.LastIndexedEtags, key, out version);

			indexStats["lastEtag"] = etag.ToByteArray();
			indexStats["lastTimestamp"] = timestamp;

			tableStorage.LastIndexedEtags.Add(writeBatch.Value, key, indexStats, version);
		}

		public void UpdateLastReduced(int id, Etag etag, DateTime timestamp)
		{
			var key = CreateKey(id);

			ushort version;
			var reduceStats = Load(tableStorage.ReduceStats, key, out version);

			if (Buffers.Compare(reduceStats.Value<byte[]>("lastReducedEtag"), etag.ToByteArray()) >= 0)
				return;

			reduceStats["lastReducedEtag"] = etag.ToByteArray();
			reduceStats["lastReducedTimestamp"] = timestamp;

			tableStorage.ReduceStats.Add(writeBatch.Value, key, reduceStats, version);
		}

		public void TouchIndexEtag(int id)
		{
			var key = CreateKey(id);

			ushort version;
			var index = Load(tableStorage.IndexingStats, key, out version);

			index["touches"] = index.Value<int>("touches") + 1;

			tableStorage.IndexingStats.Add(writeBatch.Value, key, index, version);
		}

		public void UpdateIndexingStats(int id, IndexingWorkStats stats)
		{
			var key = CreateKey(id);

			ushort version;
			var index = Load(tableStorage.IndexingStats, key, out version);

			index["attempts"] = index.Value<int>("attempts") + stats.IndexingAttempts;
			index["successes"] = index.Value<int>("successes") + stats.IndexingSuccesses;
			index["failures"] = index.Value<int>("failures") + stats.IndexingErrors;
			index["lastIndexingTime"] = SystemTime.UtcNow;

			tableStorage.IndexingStats.Add(writeBatch.Value, key, index, version);
		}

		public void UpdateReduceStats(int id, IndexingWorkStats stats)
		{
			var key = CreateKey(id);

			ushort version;
			var reduceStats = Load(tableStorage.ReduceStats, key, out version);

			reduceStats["reduce_attempts"] = reduceStats.Value<int>("reduce_attempts") + stats.ReduceAttempts;
			reduceStats["reduce_successes"] = reduceStats.Value<int>("reduce_successes") + stats.ReduceSuccesses;
			reduceStats["reduce_failures"] = reduceStats.Value<int>("reduce_failures") + stats.ReduceErrors;

			tableStorage.ReduceStats.Add(writeBatch.Value, key, reduceStats, version);
		}

		public void RemoveAllDocumentReferencesFrom(string key)
		{
			RemoveDocumentReferenceByKey(key);
		}

		public void RemoveAllDocumentReferencesByView(int view)
		{
			var documentReferencesByView = tableStorage.DocumentReferences.GetIndex(Tables.DocumentReferences.Indices.ByView);

			using (var iterator = documentReferencesByView.MultiRead(Snapshot, CreateKey(view)))
			{
				if (iterator.Seek(Slice.BeforeAllKeys))
				{
					do
					{
						RemoveDocumentReference(iterator.CurrentKey.Clone());
					}
					while (iterator.MoveNext());
				}
			}
		}

		public void UpdateDocumentReferences(int id, string key, HashSet<string> references)
		{
			var documentReferencesByKey = tableStorage.DocumentReferences.GetIndex(Tables.DocumentReferences.Indices.ByKey);
			var documentReferencesByRef = tableStorage.DocumentReferences.GetIndex(Tables.DocumentReferences.Indices.ByRef);
			var documentReferencesByView = tableStorage.DocumentReferences.GetIndex(Tables.DocumentReferences.Indices.ByView);
			var documentReferencesByViewAndKey = tableStorage.DocumentReferences.GetIndex(Tables.DocumentReferences.Indices.ByViewAndKey);

			using (var iterator = documentReferencesByViewAndKey.MultiRead(Snapshot, CreateKey(id, key)))
			{
				if (iterator.Seek(Slice.BeforeAllKeys))
				{
					do
					{
						RemoveDocumentReference(iterator.CurrentKey.Clone());
					}
					while (iterator.MoveNext());
				}
			}

			foreach (var reference in references)
			{
				var newKey = generator.CreateSequentialUuid(UuidType.DocumentReferences);
				var newKeyAsString = newKey.ToString();
				var value = new RavenJObject
				            {
					            { "view", id }, 
								{ "key", key }, 
								{ "ref", reference }
				            };

				tableStorage.DocumentReferences.Add(writeBatch.Value, newKeyAsString, value);
				documentReferencesByKey.MultiAdd(writeBatch.Value, CreateKey(key), newKeyAsString);
				documentReferencesByRef.MultiAdd(writeBatch.Value, CreateKey(reference), newKeyAsString);
				documentReferencesByView.MultiAdd(writeBatch.Value, CreateKey(id), newKeyAsString);
				documentReferencesByViewAndKey.MultiAdd(writeBatch.Value, CreateKey(id, key), newKeyAsString);
			}
		}

		public IEnumerable<string> GetDocumentsReferencing(string reference)
		{
			var documentReferencesByRef = tableStorage.DocumentReferences.GetIndex(Tables.DocumentReferences.Indices.ByRef);

			using (var iterator = documentReferencesByRef.MultiRead(Snapshot, CreateKey(reference)))
			{
				var result = new List<string>();

				if (!iterator.Seek(Slice.BeforeAllKeys))
					return result;

				do
				{
					ushort version;
					var value = LoadJson(tableStorage.DocumentReferences, iterator.CurrentKey, writeBatch.Value, out version);

					result.Add(value.Value<string>("key"));
				}
				while (iterator.MoveNext());

				return result.Distinct(StringComparer.OrdinalIgnoreCase);
			}
		}

		public int GetCountOfDocumentsReferencing(string reference)
		{
			var documentReferencesByRef = tableStorage.DocumentReferences.GetIndex(Tables.DocumentReferences.Indices.ByRef);

			using (var iterator = documentReferencesByRef.MultiRead(Snapshot, CreateKey(reference)))
			{
				var count = 0;

				if (!iterator.Seek(Slice.BeforeAllKeys)) return
					count;

				do
				{
					count++;
				}
				while (iterator.MoveNext());

				return count;
			}
		}

		public IEnumerable<string> GetDocumentsReferencesFrom(string key)
		{
			var documentReferencesByKey = tableStorage.DocumentReferences.GetIndex(Tables.DocumentReferences.Indices.ByKey);

			using (var iterator = documentReferencesByKey.MultiRead(Snapshot, CreateKey(key)))
			{
				var result = new List<string>();

				if (!iterator.Seek(Slice.BeforeAllKeys))
					return result;

				do
				{
					ushort version;
					var value = LoadJson(tableStorage.DocumentReferences, iterator.CurrentKey, writeBatch.Value, out version);

					result.Add(value.Value<string>("ref"));
				}
				while (iterator.MoveNext());

				return result.Distinct(StringComparer.OrdinalIgnoreCase);
			}
		}

		private RavenJObject Load(Table table, string name, out ushort version)
		{
			var value = LoadJson(table, CreateKey(name), writeBatch.Value, out version);
			if (value == null)
				throw new IndexDoesNotExistsException(string.Format("There is no index with the name: '{0}'", name));

			return value;
		}

		private static IndexStats GetIndexStats(RavenJToken indexingStats, RavenJToken reduceStats, RavenJToken lastIndexedEtags)
		{
			return new IndexStats
			{
				TouchCount = indexingStats.Value<int>("touches"),
				IndexingAttempts = indexingStats.Value<int>("attempts"),
				IndexingErrors = indexingStats.Value<int>("failures"),
				IndexingSuccesses = indexingStats.Value<int>("successes"),
				ReduceIndexingAttempts = reduceStats.Value<int?>("reduce_attempts"),
				ReduceIndexingErrors = reduceStats.Value<int?>("reduce_failures"),
				ReduceIndexingSuccesses = reduceStats.Value<int?>("reduce_successes"),
				Id = indexingStats.Value<int>("index"),
				Priority = (IndexingPriority)indexingStats.Value<int>("priority"),
				LastIndexedEtag = Etag.Parse(lastIndexedEtags.Value<byte[]>("lastEtag")),
				LastIndexedTimestamp = lastIndexedEtags.Value<DateTime>("lastTimestamp"),
				CreatedTimestamp = indexingStats.Value<DateTime>("createdTimestamp"),
				LastIndexingTime = indexingStats.Value<DateTime>("lastIndexingTime"),
				LastReducedEtag =
					reduceStats.Value<byte[]>("lastReducedEtag") != null
						? Etag.Parse(reduceStats.Value<byte[]>("lastReducedEtag"))
						: null,
				LastReducedTimestamp = reduceStats.Value<DateTime?>("lastReducedTimestamp")
			};
		}

		private void RemoveDocumentReferenceByKey(Slice key)
		{
			var k = CreateKey(key);

			var documentReferencesByKey = tableStorage.DocumentReferences.GetIndex(Tables.DocumentReferences.Indices.ByKey);

			using (var iterator = documentReferencesByKey.MultiRead(Snapshot, k))
			{
				if (!iterator.Seek(Slice.BeforeAllKeys))
					return;

				do
				{
					RemoveDocumentReference(iterator.CurrentKey.Clone());
				}
				while (iterator.MoveNext());
			}
		}

		private void RemoveDocumentReference(Slice id)
		{
			var documentReferencesByKey = tableStorage.DocumentReferences.GetIndex(Tables.DocumentReferences.Indices.ByKey);
			var documentReferencesByRef = tableStorage.DocumentReferences.GetIndex(Tables.DocumentReferences.Indices.ByRef);
			var documentReferencesByView = tableStorage.DocumentReferences.GetIndex(Tables.DocumentReferences.Indices.ByView);
			var documentReferencesByViewAndKey = tableStorage.DocumentReferences.GetIndex(Tables.DocumentReferences.Indices.ByViewAndKey);

			ushort version;
			var value = LoadJson(tableStorage.DocumentReferences, id, writeBatch.Value, out version);
			var reference = value.Value<string>("ref");
			var view = value.Value<string>("view");
			var key = value.Value<string>("key");

			tableStorage.DocumentReferences.Delete(writeBatch.Value, id);
			documentReferencesByKey.MultiDelete(writeBatch.Value, CreateKey(key), id);
			documentReferencesByRef.MultiDelete(writeBatch.Value, CreateKey(reference), id);
			documentReferencesByView.MultiDelete(writeBatch.Value, CreateKey(view), id);
			documentReferencesByViewAndKey.MultiDelete(writeBatch.Value, CreateKey(view, key), id);
		}
	}
}
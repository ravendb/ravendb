// -----------------------------------------------------------------------
//  <copyright file="IndexingStorageActions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Raven.Abstractions.Util.Streams;
using Raven.Database.Storage.Voron.StorageActions.Structs;

namespace Raven.Database.Storage.Voron.StorageActions
{
	using System;
	using System.Collections.Generic;
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

	internal class IndexingStorageActions : StorageActionsBase, IIndexingStorageActions
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
					ushort version;
					var indexStats = indexingStatsIterator.ReadStructForCurrent<VoronIndexingWorkStats>();
					
					var reduceStats = LoadStruct<VoronReducingWorkStats>(tableStorage.ReduceStats, key, out version);
					var lastIndexedEtag = LoadStruct<VoronLastIndexedStats>(tableStorage.LastIndexedEtags, key, out version);
					var priority = ReadPriority(key);
					var touches = ReadTouches(key);

					yield return GetIndexStats(indexStats, reduceStats, lastIndexedEtag, priority, touches);
				}
				while (indexingStatsIterator.MoveNext());
			}
		}

		public IndexStats GetIndexStats(int id)
		{
			var key = CreateKey(id);

			ushort version;

			var indexStats = LoadStruct<VoronIndexingWorkStats>(tableStorage.IndexingStats, key, out version);
			var reduceStats = LoadStruct<VoronReducingWorkStats>(tableStorage.ReduceStats, key, out version);
			var lastIndexedEtags = LoadStruct<VoronLastIndexedStats>(tableStorage.LastIndexedEtags, key, out version);
			var priority = ReadPriority(key);
			var touches = ReadTouches(key);

			return GetIndexStats(indexStats, reduceStats, lastIndexedEtags, priority, touches);
		}

		public void AddIndex(int id, bool createMapReduce)
		{
			var key = CreateKey(id);

			if (tableStorage.IndexingStats.Contains(Snapshot, key, writeBatch.Value))
				throw new ArgumentException(string.Format("There is already an index with the name: '{0}'", id));

			tableStorage.IndexingStats.AddStruct(
				writeBatch.Value,
				key,
				new VoronIndexingWorkStats
				{
					IndexId = id,
					IndexingAttempts = 0,
					IndexingSuccesses = 0,
					IndexingErrors = 0,
					CreatedTimestampTicks = SystemTime.UtcNow.Ticks,
					LastIndexingTimeTicks = DateTime.MinValue.Ticks,
				}, 0);

			tableStorage.IndexingMetadata.Add(writeBatch.Value, CreateKey(id, "priority"), BitConverter.GetBytes(1), 0);
			tableStorage.IndexingMetadata.Increment(writeBatch.Value, CreateKey(id, "touches"), 0, 0);

			tableStorage.ReduceStats.AddStruct(
				writeBatch.Value,
				key,
				new VoronReducingWorkStats
				{
					ReduceAttempts = createMapReduce ? 0 : -1,
					ReduceSuccesses = createMapReduce ? 0 : -1,
					ReduceErrors = createMapReduce ? 0 : -1,
					LastReducedEtag = createMapReduce ? new VoronEtagStruct(Etag.Empty) : new VoronEtagStruct(Etag.InvalidEtag),
					LastReducedTimestampTicks = createMapReduce ? DateTime.MinValue.Ticks : -1
				}, 0);

			tableStorage.LastIndexedEtags.AddStruct(
				writeBatch.Value,
				key,
				new VoronLastIndexedStats
				{
					IndexId = id,
					LastEtag = new VoronEtagStruct(Etag.Empty),
					LastTimestampTicks = DateTime.MinValue.Ticks
				}, 0);
		}

		public void PrepareIndexForDeletion(int id)
		{
			var key = CreateKey(id);

			tableStorage.IndexingStats.Delete(writeBatch.Value, key);
			tableStorage.IndexingMetadata.Delete(writeBatch.Value, CreateKey(id, "priority"));
			tableStorage.IndexingMetadata.Delete(writeBatch.Value, CreateKey(id, "touches"));
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
			tableStorage.IndexingMetadata.Add(writeBatch.Value, CreateKey(id, "priority"), BitConverter.GetBytes((int)priority));
		}

		public IndexFailureInformation GetFailureRate(int id)
		{
			var key = CreateKey(id);

			ushort version;
			var indexStats = LoadStruct<VoronIndexingWorkStats>(tableStorage.IndexingStats, key, out version);
			var reduceStats = LoadStruct<VoronReducingWorkStats>(tableStorage.ReduceStats, key, out version);

			var indexFailureInformation = new IndexFailureInformation
			{
				Attempts = indexStats.IndexingAttempts,
				Errors = indexStats.IndexingErrors,
				Successes = indexStats.IndexingSuccesses,
				ReduceAttempts = reduceStats.ReduceAttempts == -1 ? (int?) null : reduceStats.ReduceAttempts,
				ReduceErrors = reduceStats.ReduceErrors == -1 ? (int?) null : reduceStats.ReduceErrors,
				ReduceSuccesses = reduceStats.ReduceSuccesses == -1 ? (int?) null : reduceStats.ReduceSuccesses,
				Id = indexStats.IndexId
			};

			return indexFailureInformation;
		}

		public void UpdateLastIndexed(int id, Etag etag, DateTime timestamp)
		{
			var key = CreateKey(id);

			var version = tableStorage.LastIndexedEtags.ReadVersion(Snapshot, key);

			if(version == null)
				throw new IndexDoesNotExistsException(string.Format("There is no index with the name: '{0}'", id));

			var indexStats = new VoronLastIndexedStats
			{
				IndexId = id,
				LastEtag = new VoronEtagStruct(etag),
				LastTimestampTicks = timestamp.Ticks
			};

			tableStorage.LastIndexedEtags.AddStruct(writeBatch.Value, key, indexStats, version);
		}

		public void UpdateLastReduced(int id, Etag etag, DateTime timestamp)
		{
			var key = CreateKey(id);

			ushort version;
			var reduceStats = LoadStruct<VoronReducingWorkStats>(tableStorage.ReduceStats, key, out version);

			if (reduceStats.LastReducedEtag.ToEtag().CompareTo(etag) >= 0)
				return;

			reduceStats.LastReducedEtag = new VoronEtagStruct(etag);
			reduceStats.LastReducedTimestampTicks = timestamp.Ticks;

			tableStorage.ReduceStats.AddStruct(writeBatch.Value, key, reduceStats, version);
		}

		public void TouchIndexEtag(int id)
		{
			tableStorage.IndexingMetadata.Increment(writeBatch.Value, CreateKey(id, "touches"), 1);
		}

		public void UpdateIndexingStats(int id, IndexingWorkStats stats)
		{
			var key = CreateKey(id);

			ushort version;
			var index = LoadStruct<VoronIndexingWorkStats>(tableStorage.IndexingStats, key, out version);

			index.IndexingAttempts += stats.IndexingAttempts;
			index.IndexingSuccesses += stats.IndexingSuccesses;
			index.IndexingErrors += stats.IndexingErrors;
			index.LastIndexingTimeTicks = SystemTime.UtcNow.Ticks;

			tableStorage.IndexingStats.AddStruct(writeBatch.Value, key, index, version);
		}

		public void UpdateReduceStats(int id, IndexingWorkStats stats)
		{
			var key = CreateKey(id);

			ushort version;
			var reduceStats = LoadStruct<VoronReducingWorkStats>(tableStorage.ReduceStats, key, out version);

			reduceStats.ReduceAttempts += stats.ReduceAttempts;
			reduceStats.ReduceSuccesses += stats.ReduceSuccesses;
			reduceStats.ReduceErrors += stats.ReduceErrors;

			tableStorage.ReduceStats.AddStruct(writeBatch.Value, key, reduceStats, version);
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
						currentStorageActionsAccessor.General.MaybePulseTransaction();
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

			    if (!iterator.Seek(Slice.BeforeAllKeys))
			        yield break;

                var result = new HashSet<string>();
                do
				{
					ushort version;
					var value = LoadJson(tableStorage.DocumentReferences, iterator.CurrentKey, writeBatch.Value, out version);

				    var item = value.Value<string>("key");
				    if (result.Add(item))
				        yield return item;
				}
				while (iterator.MoveNext());
			}
		}

		public int GetCountOfDocumentsReferencing(string reference)
		{
			var documentReferencesByRef = tableStorage.DocumentReferences.GetIndex(Tables.DocumentReferences.Indices.ByRef);

			using (var iterator = documentReferencesByRef.MultiRead(Snapshot, CreateKey(reference)))
			{
				var count = 0;

				if (!iterator.Seek(Slice.BeforeAllKeys)) 
					return count;

				do
				{
					count++;
				}
				while (iterator.MoveNext());

				return count;
			}
		}

		public Dictionary<string,int> GetDocumentReferencesStats()
		{
			var documentReferencesByRef = tableStorage.DocumentReferences.GetIndex(Tables.DocumentReferences.Indices.ByRef);
			var results = new Dictionary<string, int>();
			using (var outerIterator = documentReferencesByRef.Iterate(Snapshot, null))
			{
				if (outerIterator.Seek(Slice.BeforeAllKeys) == false)
					return results;
				do
				{
					using (var iterator = documentReferencesByRef.MultiRead(Snapshot, outerIterator.CurrentKey))
					{
						var count = 0;

						if (!iterator.Seek(Slice.BeforeAllKeys))
							continue;

						do
						{
							count++;
						}
						while (iterator.MoveNext());

						results[outerIterator.CurrentKey.ToString()] = count;
					}
					
				} while (outerIterator.MoveNext());
			}

			return results;
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

		private T LoadStruct<T>(Table table, string name, out ushort version) where T : struct 
		{
			T value;

			if(TryLoadStruct(table, CreateKey(name), writeBatch.Value, out value, out version) == false)
				throw new IndexDoesNotExistsException(string.Format("There is no index with the name: '{0}'", name));

			return value;
		}

		private static IndexStats GetIndexStats(VoronIndexingWorkStats indexingStats, VoronReducingWorkStats reduceStats, VoronLastIndexedStats lastIndexedEtags, int priority, int touches)
		{
			var lastReducedEtag = reduceStats.LastReducedEtag.ToEtag();

			return new IndexStats
			{
				TouchCount = touches,
				IndexingAttempts = indexingStats.IndexingAttempts,
				IndexingErrors = indexingStats.IndexingErrors,
				IndexingSuccesses = indexingStats.IndexingSuccesses,
				ReduceIndexingAttempts = reduceStats.ReduceAttempts == -1 ? (int?) null : reduceStats.ReduceAttempts,
				ReduceIndexingErrors = reduceStats.ReduceErrors == -1 ? (int?) null : reduceStats.ReduceErrors,
				ReduceIndexingSuccesses = reduceStats.ReduceSuccesses == -1 ? (int?) null : reduceStats.ReduceSuccesses,
				Id = indexingStats.IndexId,
				Priority = (IndexingPriority)priority,
				LastIndexedEtag = lastIndexedEtags.LastEtag.ToEtag(),
				LastIndexedTimestamp = new DateTime(lastIndexedEtags.LastTimestampTicks, DateTimeKind.Utc),
				CreatedTimestamp = new DateTime(indexingStats.CreatedTimestampTicks, DateTimeKind.Utc),
				LastIndexingTime = new DateTime(indexingStats.LastIndexingTimeTicks, DateTimeKind.Utc),
				LastReducedEtag =
					lastReducedEtag.CompareTo(Etag.InvalidEtag) != 0
						? lastReducedEtag
						: null,
				LastReducedTimestamp = reduceStats.LastReducedTimestampTicks == -1 ? (DateTime?) null :  new DateTime(reduceStats.LastReducedTimestampTicks, DateTimeKind.Utc)
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

		private int ReadPriority(string key)
		{
			var readResult = tableStorage.IndexingMetadata.Read(Snapshot, CreateKey(key, "priority"), writeBatch.Value);
			if (readResult == null)
				return -1;
			return readResult.Reader.ReadLittleEndianInt32();
		}

		private int ReadTouches(string key)
		{
			var readResult = tableStorage.IndexingMetadata.Read(Snapshot, CreateKey(key, "touches"), writeBatch.Value);
			if (readResult == null)
				return  -1;
			return readResult.Reader.ReadLittleEndianInt32();
		}
	}
}
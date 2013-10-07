// -----------------------------------------------------------------------
//  <copyright file="IndexingStorageActions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
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

		private readonly WriteBatch writeBatch;

		private readonly IUuidGenerator generator;

		public IndexingStorageActions(TableStorage tableStorage, IUuidGenerator generator, SnapshotReader snapshot, WriteBatch writeBatch)
			: base(snapshot)
		{
			this.tableStorage = tableStorage;
			this.generator = generator;
			this.writeBatch = writeBatch;
		}

		public void Dispose()
		{
		}

		public IEnumerable<IndexStats> GetIndexesStats()
		{
			using (var indexingStatsIterator = tableStorage.IndexingStats.Iterate(Snapshot, writeBatch))
			using (var lastIndexedEtagIterator = tableStorage.LastIndexedEtags.Iterate(Snapshot, writeBatch))
			{
				if (!indexingStatsIterator.Seek(Slice.BeforeAllKeys))
					yield break;

				lastIndexedEtagIterator.Seek(Slice.BeforeAllKeys);

				do
				{
					var indexStats = indexingStatsIterator
						.CreateStreamForCurrent()
						.ToJObject();

					lastIndexedEtagIterator.Seek(indexingStatsIterator.CurrentKey);

					var lastIndexedEtags = lastIndexedEtagIterator
						.CreateStreamForCurrent()
						.ToJObject();

					yield return GetIndexStats(indexStats, lastIndexedEtags);
				}
				while (indexingStatsIterator.MoveNext());
			}
		}

		public IndexStats GetIndexStats(string name)
		{
			var key = CreateKey(name);

			ushort indexStatsVersion;
			ushort lastIndexedEtagsVersion;

			var indexStats = Load(tableStorage.IndexingStats, key, out indexStatsVersion);
			var lastIndexedEtags = Load(tableStorage.LastIndexedEtags, key, out lastIndexedEtagsVersion);

			return GetIndexStats(indexStats, lastIndexedEtags);
		}

		public void AddIndex(string name, bool createMapReduce)
		{
			var key = CreateKey(name);

			if (tableStorage.IndexingStats.Contains(Snapshot, key, writeBatch))
				throw new ArgumentException(string.Format("There is already an index with the name: '{0}'", name));

			tableStorage.IndexingStats.Add(
				writeBatch,
				key,
				new RavenJObject
				{
					{ "index", name },
					{ "attempts", 0 },
					{ "successes", 0 },
					{ "failures", 0 },
					{ "priority", 1 },
					{ "touches", 0 },
					{ "createdTimestamp", SystemTime.UtcNow },
					{ "lastIndexingTime", DateTime.MinValue },
					{ "reduce_attempts", createMapReduce ? 0 : (RavenJToken)RavenJValue.Null },
					{ "reduce_successes", createMapReduce ? 0 : (RavenJToken)RavenJValue.Null },
					{ "reduce_failures", createMapReduce ? 0 : (RavenJToken)RavenJValue.Null },
					{ "lastReducedEtag", createMapReduce ? Etag.Empty.ToByteArray() : (RavenJToken)RavenJValue.Null },
					{ "lastReducedTimestamp", createMapReduce ? DateTime.MinValue : (RavenJToken)RavenJValue.Null }
				}, 0);

			tableStorage.LastIndexedEtags.Add(
				writeBatch,
				key,
				new RavenJObject
				{
					{ "index", name },
					{ "lastEtag", Etag.Empty.ToByteArray() },
					{ "lastTimestamp", DateTime.MinValue },
				}, 0);
		}

		public void DeleteIndex(string name, CancellationToken token)
		{
			token.ThrowIfCancellationRequested();

			var key = CreateKey(name);

			tableStorage.IndexingStats.Delete(writeBatch, key);
			tableStorage.LastIndexedEtags.Delete(writeBatch, key);
		}

		public void SetIndexPriority(string name, IndexingPriority priority)
		{
			var key = CreateKey(name);

			ushort version;
			var index = Load(tableStorage.IndexingStats, key, out version);

			index["priority"] = (int)priority;

			tableStorage.IndexingStats.Add(writeBatch, key, index, version);
		}

		public IndexFailureInformation GetFailureRate(string name)
		{
			var key = CreateKey(name);

			ushort version;
			var index = Load(tableStorage.IndexingStats, key, out version);

			var indexFailureInformation = new IndexFailureInformation
			{
				Attempts = index.Value<int>("attempts"),
				Errors = index.Value<int>("failures"),
				Successes = index.Value<int>("successes"),
				ReduceAttempts = index.Value<int?>("reduce_attempts"),
				ReduceErrors = index.Value<int?>("reduce_failures"),
				ReduceSuccesses = index.Value<int?>("reduce_successes"),
				Name = index.Value<string>("index"),
			};

			return indexFailureInformation;
		}

		public void UpdateLastIndexed(string name, Etag etag, DateTime timestamp)
		{
			var key = CreateKey(name);

			ushort version;
			var index = Load(tableStorage.LastIndexedEtags, key, out version);

			if (Buffers.Compare(index.Value<byte[]>("lastEtag"), etag.ToByteArray()) >= 0)
				return;

			index["lastEtag"] = etag.ToByteArray();
			index["lastTimestamp"] = timestamp;

			tableStorage.LastIndexedEtags.Add(writeBatch, key, index, version);
		}

		public void UpdateLastReduced(string name, Etag etag, DateTime timestamp)
		{
			var key = CreateKey(name);

			ushort version;
			var index = Load(tableStorage.IndexingStats, key, out version);

			if (Buffers.Compare(index.Value<byte[]>("lastReducedEtag"), etag.ToByteArray()) >= 0)
				return;

			index["lastReducedEtag"] = etag.ToByteArray();
			index["lastReducedTimestamp"] = timestamp;

			tableStorage.IndexingStats.Add(writeBatch, key, index, version);
		}

		public void TouchIndexEtag(string name)
		{
			var key = CreateKey(name);

			ushort version;
			var index = Load(tableStorage.IndexingStats, key, out version);

			index["touches"] = index.Value<int>("touches") + 1;

			tableStorage.IndexingStats.Add(writeBatch, key, index, version);
		}

		public void UpdateIndexingStats(string name, IndexingWorkStats stats)
		{
			var key = CreateKey(name);

			ushort version;
			var index = Load(tableStorage.IndexingStats, key, out version);

			index["attempts"] = index.Value<int>("attempts") + stats.IndexingAttempts;
			index["successes"] = index.Value<int>("successes") + stats.IndexingSuccesses;
			index["failures"] = index.Value<int>("failures") + stats.IndexingErrors;
			index["lastIndexingTime"] = SystemTime.UtcNow;

			tableStorage.IndexingStats.Add(writeBatch, key, index, version);
		}

		public void UpdateReduceStats(string name, IndexingWorkStats stats)
		{
			var key = CreateKey(name);

			ushort version;
			var index = Load(tableStorage.IndexingStats, key, out version);

			index["reduce_attempts"] = index.Value<int>("reduce_attempts") + stats.ReduceAttempts;
			index["reduce_successes"] = index.Value<int>("reduce_successes") + stats.ReduceSuccesses;
			index["reduce_failures"] = index.Value<int>("reduce_failures") + stats.ReduceErrors;

			tableStorage.IndexingStats.Add(writeBatch, key, index, version);
		}

		public void RemoveAllDocumentReferencesFrom(string key)
		{
			RemoveDocumentReferenceByKey(key);
		}

		public void UpdateDocumentReferences(string view, string key, HashSet<string> references)
		{
			var documentReferencesByKey = tableStorage.DocumentReferences.GetIndex(Tables.DocumentReferences.Indices.ByKey);
			var documentReferencesByRef = tableStorage.DocumentReferences.GetIndex(Tables.DocumentReferences.Indices.ByRef);
			var documentReferencesByView = tableStorage.DocumentReferences.GetIndex(Tables.DocumentReferences.Indices.ByView);
			var documentReferencesByViewAndKey = tableStorage.DocumentReferences.GetIndex(Tables.DocumentReferences.Indices.ByViewAndKey);

			using (var iterator = documentReferencesByViewAndKey.MultiRead(Snapshot, CreateKey(view, key)))
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
					            { "view", view }, 
								{ "key", key }, 
								{ "ref", reference }
				            };

				tableStorage.DocumentReferences.Add(writeBatch, newKeyAsString, value);
				documentReferencesByKey.MultiAdd(writeBatch, CreateKey(key), newKeyAsString);
				documentReferencesByRef.MultiAdd(writeBatch, CreateKey(reference), newKeyAsString);
				documentReferencesByView.MultiAdd(writeBatch, CreateKey(view), newKeyAsString);
				documentReferencesByViewAndKey.MultiAdd(writeBatch, CreateKey(view, key), newKeyAsString);
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
					var value = LoadJson(tableStorage.DocumentReferences, iterator.CurrentKey, writeBatch, out version);

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
					var value = LoadJson(tableStorage.DocumentReferences, iterator.CurrentKey, writeBatch, out version);

					result.Add(value.Value<string>("ref"));
				}
				while (iterator.MoveNext());

				return result.Distinct(StringComparer.OrdinalIgnoreCase);
			}
		}

		private RavenJObject Load(Table table, string name, out ushort version)
		{
			var value = LoadJson(table, CreateKey(name), writeBatch, out version);
			if (value == null)
				throw new IndexDoesNotExistsException(string.Format("There is no index with the name: '{0}'", name));

			return value;
		}

		private static IndexStats GetIndexStats(RavenJToken indexingStats, RavenJToken lastIndexedEtags)
		{
			return new IndexStats
			{
				TouchCount = indexingStats.Value<int>("touches"),
				IndexingAttempts = indexingStats.Value<int>("attempts"),
				IndexingErrors = indexingStats.Value<int>("failures"),
				IndexingSuccesses = indexingStats.Value<int>("successes"),
				ReduceIndexingAttempts = indexingStats.Value<int?>("reduce_attempts"),
				ReduceIndexingErrors = indexingStats.Value<int?>("reduce_failures"),
				ReduceIndexingSuccesses = indexingStats.Value<int?>("reduce_successes"),
				Name = indexingStats.Value<string>("index"),
				Priority = (IndexingPriority)indexingStats.Value<int>("priority"),
				LastIndexedEtag = Etag.Parse(lastIndexedEtags.Value<byte[]>("lastEtag")),
				LastIndexedTimestamp = lastIndexedEtags.Value<DateTime>("lastTimestamp"),
				CreatedTimestamp = indexingStats.Value<DateTime>("createdTimestamp"),
				LastIndexingTime = indexingStats.Value<DateTime>("lastIndexingTime"),
				LastReducedEtag =
					indexingStats.Value<byte[]>("lastReducedEtag") != null
						? Etag.Parse(indexingStats.Value<byte[]>("lastReducedEtag"))
						: null,
				LastReducedTimestamp = indexingStats.Value<DateTime?>("lastReducedTimestamp")
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
			var value = LoadJson(tableStorage.DocumentReferences, id, writeBatch, out version);
			var reference = value.Value<string>("ref");
			var view = value.Value<string>("view");
			var key = value.Value<string>("key");

			tableStorage.DocumentReferences.Delete(writeBatch, id);
			documentReferencesByKey.MultiDelete(writeBatch, CreateKey(key), id);
			documentReferencesByRef.MultiDelete(writeBatch, CreateKey(reference), id);
			documentReferencesByView.MultiDelete(writeBatch, CreateKey(view), id);
			documentReferencesByViewAndKey.MultiDelete(writeBatch, CreateKey(view, key), id);
		}
	}
}
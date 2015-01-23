
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Util.Streams;
using Raven.Database.Storage.Voron.StorageActions.Structs;
using Raven.Database.Util.Streams;

namespace Raven.Database.Storage.Voron.StorageActions
{
	using global::Voron;
	using global::Voron.Impl;
	using Raven.Abstractions.Data;
	using Raven.Abstractions.Exceptions;
	using Raven.Database.Storage.Voron.Impl;
	using System;

	internal class StalenessStorageActions : StorageActionsBase, IStalenessStorageActions
	{
		private readonly TableStorage tableStorage;
		private readonly Reference<WriteBatch> writeBatch;

		public StalenessStorageActions(TableStorage tableStorage, Reference<SnapshotReader> snapshot, Reference<WriteBatch> writeBatch, IBufferPool bufferPool)
			: base(snapshot, bufferPool)
		{
			this.tableStorage = tableStorage;
			this.writeBatch = writeBatch;
		}

		public bool IsIndexStale(int id, DateTime? cutOff, Etag cutoffEtag)
		{
			var key = CreateKey(id);

			ushort version;
			VoronIndexingWorkStats indexingStats;
			if (TryLoadStruct(tableStorage.IndexingStats, key, writeBatch.Value, out indexingStats, out version) == false)
				return false; // index does not exists

			VoronReducingWorkStats reduceStats;
			if (TryLoadStruct(tableStorage.ReduceStats, key, writeBatch.Value, out reduceStats, out version) == false)
				throw new ArgumentException("reduceStats");

			var hasReduce = reduceStats.LastReducedEtag.Restarts != Etag.InvalidEtag.Restarts && reduceStats.LastReducedEtag.Changes != Etag.InvalidEtag.Changes;

			if (IsMapStale(id) || hasReduce && IsReduceStale(id))
			{
				VoronLastIndexedStats lastIndexedEtags;
				
				if(TryLoadStruct(tableStorage.LastIndexedEtags, key, writeBatch.Value, out lastIndexedEtags, out version) == false)
					throw new ArgumentException("lastIndexedEtags");

				if (cutOff != null)
				{
					var lastIndexedTime = new DateTime(lastIndexedEtags.LastTimestampTicks, DateTimeKind.Utc);
					if (cutOff.Value >= lastIndexedTime)
						return true;

					var lastReducedTime = reduceStats.LastReducedTimestampTicks != -1 ? new DateTime(reduceStats.LastReducedTimestampTicks, DateTimeKind.Utc) : (DateTime?) null;

					if (lastReducedTime != null && cutOff.Value >= lastReducedTime.Value)
						return true;
				}
				else if (cutoffEtag != null)
				{
					var lastIndexedEtag = lastIndexedEtags.LastEtag.ToEtag();

					if (lastIndexedEtag.CompareTo(cutoffEtag) < 0)
						return true;
				}
				else
				{
					return true;
				}
			}

			return IsIndexStaleByTask(id, cutOff);
		}

		public bool IsIndexStaleByTask(int view, DateTime? cutOff)
		{
			ushort version;
			var key = CreateKey(view);
			var tasksByIndex = tableStorage.Tasks.GetIndex(Tables.Tasks.Indices.ByIndex);
			using (var iterator = tasksByIndex.MultiRead(Snapshot, key))
			{
				if (!iterator.Seek(Slice.BeforeAllKeys))
					return false;

				if (cutOff == null)
					return true;

				do
				{
					var value = LoadJson(tableStorage.Tasks, iterator.CurrentKey, writeBatch.Value, out version);
					var time = value.Value<DateTime>("time");

					if (time <= cutOff.Value)
						return true;
				} while (iterator.MoveNext());
			}

			return false;
		}

		public bool IsReduceStale(int id)
		{
			var scheduledReductionsByView = tableStorage.ScheduledReductions.GetIndex(Tables.ScheduledReductions.Indices.ByView);
			using (var iterator = scheduledReductionsByView.MultiRead(Snapshot, CreateKey(id)))
			{
				if (!iterator.Seek(Slice.BeforeAllKeys))
					return false;

				return true;
			}
		}

		public bool IsMapStale(int id)
		{
			var key = CreateKey(id);

			ushort version;
			VoronLastIndexedStats lastIndexed;
			if (TryLoadStruct(tableStorage.LastIndexedEtags, key, writeBatch.Value, out lastIndexed, out version) == false)
				return false;

			var lastIndexedEtag = lastIndexed.LastEtag.ToEtag();
			var lastDocumentEtag = GetMostRecentDocumentEtag();

			return lastDocumentEtag.CompareTo(lastIndexedEtag) > 0;
		}

		public Tuple<DateTime, Etag> IndexLastUpdatedAt(int id)
		{
			var key = CreateKey(id);

			ushort version;
			VoronIndexingWorkStats indexingStats;
			if (TryLoadStruct(tableStorage.IndexingStats, key, writeBatch.Value, out indexingStats, out version) == false)
				throw new IndexDoesNotExistsException("Could not find index named: " + id);

			VoronReducingWorkStats reduceStats;
			if (TryLoadStruct(tableStorage.ReduceStats, key, writeBatch.Value, out reduceStats, out version) == false)
				throw new ArgumentException("reduceStats");

			if (reduceStats.LastReducedTimestampTicks != -1)
			{
				return Tuple.Create(
					new DateTime(reduceStats.LastReducedTimestampTicks, DateTimeKind.Utc),
					reduceStats.LastReducedEtag.ToEtag());
			}

			VoronLastIndexedStats lastIndexedEtags;
			if(TryLoadStruct(tableStorage.LastIndexedEtags, key, writeBatch.Value, out lastIndexedEtags, out version) == false)
				throw new ArgumentException("lastIndexedEtags");

			return Tuple.Create(new DateTime(lastIndexedEtags.LastTimestampTicks, DateTimeKind.Utc),
				 lastIndexedEtags.LastEtag.ToEtag());
		}

		public Etag GetMostRecentDocumentEtag()
		{
			var documentsByEtag = tableStorage.Documents.GetIndex(Tables.Documents.Indices.KeyByEtag);
			using (var iterator = documentsByEtag.Iterate(Snapshot, writeBatch.Value))
			{
				if (!iterator.Seek(Slice.AfterAllKeys))
					return Etag.Empty;

				return Etag.Parse(iterator.CurrentKey.ToString());
			}
		}

        [Obsolete("Use RavenFS instead.")]
		public Etag GetMostRecentAttachmentEtag()
		{
			var attachmentsByEtag = tableStorage.Attachments.GetIndex(Tables.Attachments.Indices.ByEtag);
			using (var iterator = attachmentsByEtag.Iterate(Snapshot, writeBatch.Value))
			{
				if (!iterator.Seek(Slice.AfterAllKeys))
					return Etag.Empty;

				return Etag.Parse(iterator.CurrentKey.ToString());
			}
		}

		public int GetIndexTouchCount(int id)
		{
			var read = tableStorage.IndexingMetadata.Read(Snapshot, CreateKey(id, "touches"), writeBatch.Value);
			if (read == null)
				return -1;

			return read.Reader.ReadLittleEndianInt32();
		}
	}
}
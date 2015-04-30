
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Util.Streams;
using Raven.Database.Storage.Voron.StorageActions.StructureSchemas;

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
            var key = (Slice) CreateKey(id);

			ushort version;

			var indexingStatsReader = LoadStruct(tableStorage.IndexingStats, key, writeBatch.Value, out version);
			if (indexingStatsReader == null)
				return false; // index does not exists

			var reducingStatsReader = LoadStruct(tableStorage.ReduceStats, key, writeBatch.Value, out version);
			if (reducingStatsReader == null)
				throw new ArgumentException("reduceStats");

			var hasReduce = Etag.Parse(reducingStatsReader.ReadBytes(ReducingWorkStatsFields.LastReducedEtag)).CompareTo(Etag.InvalidEtag) != 0;

			if (IsMapStale(id) || hasReduce && IsReduceStale(id))
			{
				var lastIndexedEtagsReader = LoadStruct(tableStorage.LastIndexedEtags, key, writeBatch.Value, out version);
				if(lastIndexedEtagsReader == null)
					throw new ArgumentException("lastIndexedEtags");

				if (cutOff != null)
				{
					var lastIndexedTime = DateTime.FromBinary(lastIndexedEtagsReader.ReadLong(LastIndexedStatsFields.LastTimestamp));
					if (cutOff.Value >= lastIndexedTime)
						return true;

					var lastReducedTimestamp = reducingStatsReader.ReadLong(ReducingWorkStatsFields.LastReducedTimestamp);

					var lastReducedTime = lastReducedTimestamp != -1 ? DateTime.FromBinary(lastReducedTimestamp) : (DateTime?)null;

					if (lastReducedTime != null && cutOff.Value >= lastReducedTime.Value)
						return true;
				}
				else if (cutoffEtag != null)
				{
					var lastIndexedEtag = Etag.Parse(lastIndexedEtagsReader.ReadBytes(LastIndexedStatsFields.LastEtag));

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
            var key = (Slice)CreateKey(view);
			var tasksByIndex = tableStorage.Tasks.GetIndex(Tables.Tasks.Indices.ByIndex);
			using (var iterator = tasksByIndex.MultiRead(Snapshot, key))
			{
				if (!iterator.Seek(Slice.BeforeAllKeys))
					return false;

				if (cutOff == null)
					return true;

				do
				{
					var value = LoadStruct(tableStorage.Tasks, iterator.CurrentKey, writeBatch.Value, out version);
					var time = DateTime.FromBinary(value.ReadLong(TaskFields.AddedAt));

					if (time <= cutOff.Value)
						return true;
				} while (iterator.MoveNext());
			}

			return false;
		}

		public bool IsReduceStale(int id)
		{
			var scheduledReductionsByView = tableStorage.ScheduledReductions.GetIndex(Tables.ScheduledReductions.Indices.ByView);
            using (var iterator = scheduledReductionsByView.MultiRead(Snapshot, (Slice)CreateKey(id)))
			{
				if (!iterator.Seek(Slice.BeforeAllKeys))
					return false;

				return true;
			}
		}

		public bool IsMapStale(int id)
		{
            var key = (Slice)CreateKey(id);

			ushort version;
			var lastIndexedEtagsReader = LoadStruct(tableStorage.LastIndexedEtags, key, writeBatch.Value, out version);
			if (lastIndexedEtagsReader == null)
				return false;

			var lastIndexedEtag = Etag.Parse(lastIndexedEtagsReader.ReadBytes(LastIndexedStatsFields.LastEtag));
			var lastDocumentEtag = GetMostRecentDocumentEtag();

			return lastDocumentEtag.CompareTo(lastIndexedEtag) > 0;
		}

		public Tuple<DateTime, Etag> IndexLastUpdatedAt(int id)
		{
            var key = (Slice)CreateKey(id);

			ushort version;
			var indexingStatsReader = LoadStruct(tableStorage.IndexingStats, key, writeBatch.Value, out version);
			if (indexingStatsReader == null)
				throw new IndexDoesNotExistsException("Could not find index named: " + id);

			var reducingStatsReader = LoadStruct(tableStorage.ReduceStats, key, writeBatch.Value, out version);
			if (reducingStatsReader == null)
				throw new ArgumentException("reduceStats");

			var lastReducedTimestamp = reducingStatsReader.ReadLong(ReducingWorkStatsFields.LastReducedTimestamp);

			if (lastReducedTimestamp != -1)
			{
				return Tuple.Create(
					DateTime.FromBinary(lastReducedTimestamp),
					Etag.Parse(reducingStatsReader.ReadBytes(ReducingWorkStatsFields.LastReducedEtag)));
			}

			var lastIndexedEtagsReader = LoadStruct(tableStorage.LastIndexedEtags, key, writeBatch.Value, out version);
			if (lastIndexedEtagsReader == null)
				throw new ArgumentException("lastIndexedEtags");

			return Tuple.Create(DateTime.FromBinary(lastIndexedEtagsReader.ReadLong(LastIndexedStatsFields.LastTimestamp)),
				Etag.Parse(lastIndexedEtagsReader.ReadBytes(LastIndexedStatsFields.LastEtag)));
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
            var read = tableStorage.IndexingMetadata.Read(Snapshot, (Slice)CreateKey(id, "touches"), writeBatch.Value);
			if (read == null)
				return -1;

			return read.Reader.ReadLittleEndianInt32();
		}
	}
}
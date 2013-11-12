
namespace Raven.Database.Storage.Voron.StorageActions
{
	using global::Voron;
	using global::Voron.Impl;
	using Raven.Abstractions.Data;
	using Raven.Abstractions.Exceptions;
	using Raven.Database.Storage.Voron.Impl;
	using System;

	public class StalenessStorageActions : StorageActionsBase, IStalenessStorageActions
	{
		private readonly TableStorage tableStorage;
		private readonly WriteBatch writeBatch;

		public StalenessStorageActions(TableStorage tableStorage, SnapshotReader snapshot, WriteBatch writeBatch)
			: base(snapshot)
		{
			this.tableStorage = tableStorage;
			this.writeBatch = writeBatch;
		}

		public bool IsIndexStale(int id, DateTime? cutOff, Etag cutoffEtag)
		{
			var key = CreateKey(id);

			ushort version;
			var indexingStats = LoadJson(tableStorage.IndexingStats, key, writeBatch, out version);
			if (indexingStats == null)
				return false; // index does not exists

			var reduceStats = LoadJson(tableStorage.ReduceStats, key, writeBatch, out version);
			if (reduceStats == null)
				throw new ArgumentException("reduceStats");

			var hasReduce = reduceStats.Value<byte[]>("lastReducedEtag") != null;

			if (IsMapStale(id) || hasReduce && IsReduceStale(id))
			{
				var lastIndexedEtags = LoadJson(tableStorage.LastIndexedEtags, key, writeBatch, out version);

				if (cutOff != null)
				{
					var lastIndexedTime = lastIndexedEtags.Value<DateTime>("lastTimestamp");
					if (cutOff.Value >= lastIndexedTime)
						return true;

					var lastReducedTime = lastIndexedEtags.Value<DateTime?>("lastReducedTimestamp");
					if (lastReducedTime != null && cutOff.Value >= lastReducedTime.Value)
						return true;
				}
				else if (cutoffEtag != null)
				{
					var lastIndexedEtag = Etag.Parse(lastIndexedEtags.Value<byte[]>("lastEtag"));

					if (lastIndexedEtag.CompareTo(cutoffEtag) < 0)
						return true;
				}
				else
				{
					return true;
				}
			}

			var tasksByIndex = tableStorage.Tasks.GetIndex(Tables.Tasks.Indices.ByIndex);
			using (var iterator = tasksByIndex.MultiRead(Snapshot, key))
			{
				if (!iterator.Seek(Slice.BeforeAllKeys))
					return false;

				if (cutOff == null)
					return true;

				do
				{
					var value = LoadJson(tableStorage.Tasks, iterator.CurrentKey, writeBatch, out version);
					var time = value.Value<DateTime>("time");

					if (time <= cutOff.Value)
						return true;
				}
				while (iterator.MoveNext());
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
			var read = LoadJson(tableStorage.LastIndexedEtags, key, writeBatch, out version);
			if (read == null)
				return false;

			var lastIndexedEtag = Etag.Parse(read.Value<byte[]>("lastEtag"));
			var lastDocumentEtag = GetMostRecentDocumentEtag();

			return lastDocumentEtag.CompareTo(lastIndexedEtag) > 0;
		}

		public Tuple<DateTime, Etag> IndexLastUpdatedAt(int id)
		{
			var key = CreateKey(id);

			ushort version;
			var indexingStats = LoadJson(tableStorage.IndexingStats, key, writeBatch, out version);
			if (indexingStats == null)
				throw new IndexDoesNotExistsException("Could not find index named: " + id);

			var reduceStats = LoadJson(tableStorage.ReduceStats, key, writeBatch, out version);
			if (reduceStats == null)
				throw new ArgumentException("reduceStats");

			if (reduceStats.Value<object>("lastReducedTimestamp") != null)
			{
				return Tuple.Create(
					reduceStats.Value<DateTime>("lastReducedTimestamp"),
					Etag.Parse(reduceStats.Value<byte[]>("lastReducedEtag")));
			}

			var lastIndexedEtags = LoadJson(tableStorage.LastIndexedEtags, key, writeBatch, out version);

			return Tuple.Create(lastIndexedEtags.Value<DateTime>("lastTimestamp"),
				Etag.Parse(lastIndexedEtags.Value<byte[]>("lastEtag")));
		}

		public Etag GetMostRecentDocumentEtag()
		{
			var documentsByEtag = tableStorage.Documents.GetIndex(Tables.Documents.Indices.KeyByEtag);
			using (var iterator = documentsByEtag.Iterate(Snapshot, writeBatch))
			{
				if (!iterator.Seek(Slice.AfterAllKeys))
					return Etag.Empty;

				return Etag.Parse(iterator.CurrentKey.ToString());
			}
		}

		public Etag GetMostRecentAttachmentEtag()
		{
			var attachmentsByEtag = tableStorage.Attachments.GetIndex(Tables.Attachments.Indices.ByEtag);
			using (var iterator = attachmentsByEtag.Iterate(Snapshot, writeBatch))
			{
				if (!iterator.Seek(Slice.AfterAllKeys))
					return Etag.Empty;

				return Etag.Parse(iterator.CurrentKey.ToString());
			}
		}

		public int GetIndexTouchCount(int id)
		{
			var key = CreateKey(id);

			ushort version;
			var indexingStats = LoadJson(tableStorage.IndexingStats, key, writeBatch, out version);

			if (indexingStats == null)
				return -1;

			return indexingStats.Value<int>("touches");
		}
	}
}
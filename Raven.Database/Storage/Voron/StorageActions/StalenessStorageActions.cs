namespace Raven.Database.Storage.Voron.StorageActions
{
	using System;

	using Raven.Abstractions.Data;
	using Raven.Abstractions.Exceptions;
	using Raven.Abstractions.Extensions;
	using Raven.Database.Storage.Voron.Impl;
	using Raven.Json.Linq;

	using global::Voron;
	using global::Voron.Impl;

	public class StalenessStorageActions : IStalenessStorageActions
	{
		private readonly TableStorage tableStorage;

		private readonly SnapshotReader snapshot;

		public StalenessStorageActions(TableStorage tableStorage, SnapshotReader snapshot)
		{
			this.tableStorage = tableStorage;
			this.snapshot = snapshot;
		}

		public bool IsIndexStale(string name, DateTime? cutOff, Etag cutoffEtag)
		{
			ushort version;
			var indexingStats = this.Load(this.tableStorage.IndexingStats, name, out version);
			if (indexingStats == null)
				return false; // index does not exists

			var lastIndexedEtags = this.Load(this.tableStorage.LastIndexedEtags, name, out version);

			if (this.IsMapStale(name) || this.IsReduceStale(name))
			{
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
					var lastIndexedEtag = lastIndexedEtags.Value<byte[]>("lastEtag");

					if (Buffers.Compare(lastIndexedEtag, cutoffEtag.ToByteArray()) < 0)
						return true;
				}
				else
				{
					return true;
				}
			}

			var tasksByIndex = this.tableStorage.Tasks.GetIndex(Tables.Tasks.Indices.ByIndex);
			using (var iterator = tasksByIndex.MultiRead(this.snapshot, name))
			{
				if (!iterator.Seek(Slice.BeforeAllKeys))
					return false;

				if (cutOff == null)
					return true;

				do
				{
					using (var read = this.tableStorage.Tasks.Read(this.snapshot, iterator.CurrentKey))
					{
						var value = read.Stream.ToJObject();
						var time = value.Value<DateTime>("time");

						if (time <= cutOff.Value)
							return true;
					}
				}
				while (iterator.MoveNext());
			}

			return false;
		}

		public bool IsReduceStale(string view)
		{
			var scheduledReductionsByView = this.tableStorage.ScheduledReductions.GetIndex(Tables.ScheduledReductions.Indices.ByView);
			using (var iterator = scheduledReductionsByView.MultiRead(this.snapshot, view))
			{
				if (!iterator.Seek(Slice.BeforeAllKeys))
					return false;

				return true;
			}
		}

		public bool IsMapStale(string name)
		{
			ushort version;
			var read = this.Load(this.tableStorage.LastIndexedEtags, name, out version);
			if (read == null)
				return false;

			var lastIndexedEtag = Etag.Parse(read.Value<byte[]>("lastEtag"));
			var lastDocumentEtag = this.GetMostRecentDocumentEtag();

			return lastDocumentEtag.CompareTo(lastIndexedEtag) > 0;
		}

		public Tuple<DateTime, Etag> IndexLastUpdatedAt(string name)
		{
			ushort version;
			var indexingStats = this.Load(this.tableStorage.IndexingStats, name, out version);
			if (indexingStats == null)
				throw new IndexDoesNotExistsException("Could not find index named: " + name);

			var lastIndexedEtags = this.Load(this.tableStorage.LastIndexedEtags, name, out version);
			if (lastIndexedEtags.Value<object>("lastReducedTimestamp") != null)
			{
				return Tuple.Create(
					lastIndexedEtags.Value<DateTime>("lastReducedTimestamp"),
					Etag.Parse(lastIndexedEtags.Value<byte[]>("lastReducedEtag")));
			}

			return Tuple.Create(lastIndexedEtags.Value<DateTime>("lastTimestamp"),
				Etag.Parse(lastIndexedEtags.Value<byte[]>("lastEtag")));
		}

		public Etag GetMostRecentDocumentEtag()
		{
			var documentsByEtag = this.tableStorage.Documents.GetIndex(Tables.Documents.Indices.KeyByEtag);
			using (var iterator = documentsByEtag.Iterate(this.snapshot))
			{
				if (!iterator.Seek(Slice.AfterAllKeys))
					return Etag.Empty;

				return Etag.Parse(iterator.CurrentKey.ToString());
			}
		}

		public Etag GetMostRecentAttachmentEtag()
		{
			var attachmentsByEtag = this.tableStorage.Attachments.GetIndex(Tables.Attachments.Indices.ByEtag);
			using (var iterator = attachmentsByEtag.Iterate(this.snapshot))
			{
				if (!iterator.Seek(Slice.AfterAllKeys))
					return Etag.Empty;

				return Etag.Parse(iterator.CurrentKey.ToString());
			}
		}

		public int GetIndexTouchCount(string name)
		{
			ushort version;
			var indexingStats = this.Load(this.tableStorage.IndexingStats, name, out version);

			if (indexingStats == null)
				throw new IndexDoesNotExistsException("Could not find index named: " + name);

			return indexingStats.Value<int>("touches");
		}

		private RavenJObject Load(Table table, string name, out ushort version)
		{
			using (var read = table.Read(this.snapshot, name))
			{
				if (read == null)
				{
					version = 0;
					return null;
				}

				version = read.Version;
				return read.Stream.ToJObject();
			}
		}
	}
}
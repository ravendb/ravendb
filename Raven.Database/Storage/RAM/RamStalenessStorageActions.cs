using System;
using System.Linq;
using Raven.Database.Exceptions;

namespace Raven.Database.Storage.RAM
{
	public class RamStalenessStorageActions : IStalenessStorageActions
	{
		private readonly RamState state;

		public RamStalenessStorageActions(RamState state)
		{
			this.state = state;
		}

		public bool IsIndexStale(string name, DateTime? cutOff, Guid? cutoffEtag)
		{
			var index = state.Indexes.GetOrDefault(name);
			var indexStats = state.IndexesStats.GetOrDefault(name);

			if (index == null || indexStats == null)
				return false;


			var hasReduce = index.IsMapReduce;

			if (IsMapStale(name) || hasReduce && IsReduceStale(name))
			{
				if (cutOff != null)
				{
					var lastIndexedTimestamp = indexStats.LastIndexedTimestamp;

					if (cutOff.Value >= lastIndexedTimestamp)
						return true;

					if (hasReduce)
					{
						lastIndexedTimestamp =indexStats.LastReducedTimestamp ?? DateTime.MinValue;

						if (cutOff.Value >= lastIndexedTimestamp)
							return true;
					}
				}
				else if (cutoffEtag != null)
				{
					var lastIndexedEtag = indexStats.LastIndexedEtag;

					if (lastIndexedEtag.CompareTo(cutoffEtag.Value.ToByteArray()) < 0)
						return true;
				}
				else
				{
					return true;
				}
			}

			return false;
		}

		public bool IsReduceStale(string name)
		{
			return state.ScheduledReductions.Any(pair => pair.Key == name);

			//Api.JetSetCurrentIndex(session, ScheduledReductions, "by_view");
			//Api.MakeKey(session, ScheduledReductions, name, Encoding.Unicode, MakeKeyGrbit.NewKey);
			//return Api.TrySeek(session, ScheduledReductions, SeekGrbit.SeekEQ);
		}

		public bool IsMapStale(string name)
		{
			var index = state.IndexesStats.GetOrDefault(name);

			if (index == null)
				return false;

			var lastIndexedEtag = index.LastIndexedEtag;

			var lastEtag = state.Documents
				.OrderByDescending(pair => pair.Value.Document.Etag)
				.Select(pair => pair.Value.Document.Etag)
				.FirstOrDefault();

			if (lastEtag == null)
				return false;

			return ((Guid)lastEtag).CompareTo(lastIndexedEtag) > 0;
		}

		public Tuple<DateTime, Guid> IndexLastUpdatedAt(string name)
		{
			var index = state.Indexes.GetOrDefault(name);
			var indexStats = state.IndexesStats.GetOrDefault(name);

			if (indexStats == null || index == null)
				throw new IndexDoesNotExistsException("Could not find index named: " + name);

			if (index.IsMapReduce)
			{// for map-reduce indexes, we use the reduce stats

				var lastReducedIndex = indexStats.LastReducedTimestamp ?? DateTime.MinValue; 
				if (indexStats.LastReducedEtag != null)
				{
					var lastReducedEtag = (Guid)indexStats.LastReducedEtag;

					return Tuple.Create(lastReducedIndex, lastReducedEtag);
				}
			}


			var lastIndexedTimestamp = indexStats.LastIndexedTimestamp;
			var lastIndexedEtag = indexStats.LastIndexedEtag;

			return Tuple.Create(lastIndexedTimestamp, lastIndexedEtag);
		}

		public Guid GetMostRecentDocumentEtag()
		{
			var lastEtag = state.Documents
				.OrderByDescending(pair => pair.Value.Document.Etag)
				.Select(pair => pair.Value.Document.Etag)
				.FirstOrDefault();

			if (lastEtag == null)
				return Guid.Empty;

			return (Guid)lastEtag;
		}

		public Guid GetMostRecentAttachmentEtag()
		{
			var lastEtag = state.Attachments
				.OrderByDescending(pair => pair.Value.Etag)
				.Select(pair => pair.Value.Etag)
				.FirstOrDefault();

			return lastEtag;
		}

		public Guid? GetMostRecentReducedEtag(string name)
		{
			throw new NotImplementedException();
		}

		public int GetIndexTouchCount(string indexName)
		{
			var indexStat = state.IndexesStats.GetOrDefault(indexName);
			if (indexStat == null)
				return -1;

			return indexStat.TouchCount;
		}
	}
}

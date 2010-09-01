using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Isam.Esent.Interop;
using Raven.Database.Data;
using Raven.Database.Exceptions;
using Raven.Database.Storage.StorageActions;
using Raven.Database.Extensions;

namespace Raven.Storage.Esent.StorageActions
{
	public partial class DocumentStorageActions : IIndexingStorageActions
	{
		public void SetCurrentIndexStatsTo(string index)
		{
			Api.JetSetCurrentIndex(session, IndexesStats, "by_key");
			Api.MakeKey(session, IndexesStats, index, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, IndexesStats, SeekGrbit.SeekEQ) == false)
				throw new IndexDoesNotExistsException("There is no index named: " + index);
		}

		public void FlushIndexStats()
		{
			// nothing to do here, data will be flushed on commit
		}

		public void IncrementIndexingAttempt()
		{
			Api.EscrowUpdate(session, IndexesStats, tableColumnsCache.IndexesStatsColumns["attempts"], 1);
		}

		public void IncrementSuccessIndexing()
		{
			Api.EscrowUpdate(session, IndexesStats, tableColumnsCache.IndexesStatsColumns["successes"], 1);
		}

		public void IncrementIndexingFailure()
		{
			Api.EscrowUpdate(session, IndexesStats, tableColumnsCache.IndexesStatsColumns["errors"], 1);
		}

		public IEnumerable<IndexStats> GetIndexesStats()
		{
			Api.MoveBeforeFirst(session, IndexesStats);
			while (Api.TryMoveNext(session, IndexesStats))
			{
				yield return new IndexStats
				{
					Name = Api.RetrieveColumnAsString(session, IndexesStats, tableColumnsCache.IndexesStatsColumns["key"]),
					IndexingAttempts =
						Api.RetrieveColumnAsInt32(session, IndexesStats, tableColumnsCache.IndexesStatsColumns["attempts"]).Value,
					IndexingSuccesses =
						Api.RetrieveColumnAsInt32(session, IndexesStats, tableColumnsCache.IndexesStatsColumns["successes"]).Value,
					IndexingErrors =
						Api.RetrieveColumnAsInt32(session, IndexesStats, tableColumnsCache.IndexesStatsColumns["errors"]).Value,
					LastIndexedEtag = 
						Api.RetrieveColumn(session, IndexesStats, tableColumnsCache.IndexesStatsColumns["last_indexed_etag"]).TransfromToGuidWithProperSorting(),
					LastIndexedTimestamp= 
						Api.RetrieveColumnAsDateTime(session, IndexesStats, tableColumnsCache.IndexesStatsColumns["last_indexed_timestamp"]).Value,
				};
			}
		}

		public void AddIndex(string name)
		{
			using (var update = new Update(session, IndexesStats, JET_prep.Insert))
			{
				Api.SetColumn(session, IndexesStats, tableColumnsCache.IndexesStatsColumns["key"], name, Encoding.Unicode);
				Api.SetColumn(session, IndexesStats, tableColumnsCache.IndexesStatsColumns["last_indexed_etag"], Guid.Empty.TransformToValueForEsentSorting(),Encoding.ASCII);
				Api.SetColumn(session, IndexesStats, tableColumnsCache.IndexesStatsColumns["last_indexed_timestamp"], DateTime.MinValue);
				update.Save();
			}
		}

		public void DeleteIndex(string name)
		{
			Api.JetSetCurrentIndex(session, IndexesStats, "by_key");
			Api.MakeKey(session, IndexesStats, name, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, IndexesStats, SeekGrbit.SeekEQ) == false)
				return;
			Api.JetDelete(session, IndexesStats);
		}

		public void DecrementIndexingAttempt()
		{
			Api.EscrowUpdate(session, IndexesStats, tableColumnsCache.IndexesStatsColumns["attempts"], -1);
		}

		public IndexFailureInformation GetFailureRate(string index)
		{
			SetCurrentIndexStatsTo(index);
			return new IndexFailureInformation
			{
				Name = index,
				Attempts = Api.RetrieveColumnAsInt32(session, IndexesStats, tableColumnsCache.IndexesStatsColumns["attempts"]).Value,
				Errors = Api.RetrieveColumnAsInt32(session, IndexesStats, tableColumnsCache.IndexesStatsColumns["errors"]).Value,
				Successes = Api.RetrieveColumnAsInt32(session, IndexesStats, tableColumnsCache.IndexesStatsColumns["successes"]).Value
			};
		}

		public void UpdateLastIndexed(string index, Guid etag, DateTime timestamp)
		{
			SetCurrentIndexStatsTo(index);
			using(var update = new Update(session, IndexesStats, JET_prep.Replace))
			{
				Api.SetColumn(session, IndexesStats,
				              tableColumnsCache.IndexesStatsColumns["last_indexed_etag"],
				              etag.TransformToValueForEsentSorting(),
							  Encoding.ASCII);
				Api.SetColumn(session, IndexesStats,
							  tableColumnsCache.IndexesStatsColumns["last_indexed_timestamp"],
							  timestamp);
				update.Save();
			}
		}
	}
}
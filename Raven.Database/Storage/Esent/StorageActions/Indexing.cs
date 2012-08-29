//-----------------------------------------------------------------------
// <copyright file="Indexing.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using Microsoft.Isam.Esent.Interop;
using Raven.Abstractions.Data;
using Raven.Database.Data;
using Raven.Database.Exceptions;
using Raven.Database.Extensions;
using Raven.Database.Indexing;
using Raven.Database.Storage;

namespace Raven.Storage.Esent.StorageActions
{
	public partial class DocumentStorageActions : IIndexingStorageActions
	{
		private bool SetCurrentIndexStatsToImpl(string index)
	    {
	        Api.JetSetCurrentIndex(session, IndexesStats, "by_key");
	        Api.MakeKey(session, IndexesStats, index, Encoding.Unicode, MakeKeyGrbit.NewKey);
	        if (Api.TrySeek(session, IndexesStats, SeekGrbit.SeekEQ) == false)
	            throw new IndexDoesNotExistsException("There is no index named: " + index);

	        // this is optional
	        Api.JetSetCurrentIndex(session, IndexesStatsReduce, "by_key");
	        Api.MakeKey(session, IndexesStatsReduce, index, Encoding.Unicode, MakeKeyGrbit.NewKey);
	        return Api.TrySeek(session, IndexesStatsReduce, SeekGrbit.SeekEQ);
	    }

		public IEnumerable<IndexStats> GetIndexesStats()
		{
			Api.JetSetCurrentIndex(session, IndexesStatsReduce, "by_key");

			Api.MoveBeforeFirst(session, IndexesStats);
			while (Api.TryMoveNext(session, IndexesStats))
			{
				var indexName = Api.RetrieveColumnAsString(session, IndexesStats, tableColumnsCache.IndexesStatsColumns["key"]);
				Api.MakeKey(session, IndexesStatsReduce, indexName, Encoding.Unicode, MakeKeyGrbit.NewKey);
			    var hasReduce = Api.TrySeek(session, IndexesStatsReduce, SeekGrbit.SeekEQ);

				Api.MakeKey(session, IndexesEtags, indexName, Encoding.Unicode, MakeKeyGrbit.NewKey);
				Api.TrySeek(session, IndexesEtags, SeekGrbit.SeekEQ);

			    yield return new IndexStats
				{
					Name = indexName,
					TouchCount = Api.RetrieveColumnAsInt32(session, IndexesEtags, tableColumnsCache.IndexesEtagsColumns["touches"]).Value,
					IndexingAttempts =
						Api.RetrieveColumnAsInt32(session, IndexesStats, tableColumnsCache.IndexesStatsColumns["attempts"]).Value,
					IndexingSuccesses =
						Api.RetrieveColumnAsInt32(session, IndexesStats, tableColumnsCache.IndexesStatsColumns["successes"]).Value,
					IndexingErrors =
						Api.RetrieveColumnAsInt32(session, IndexesStats, tableColumnsCache.IndexesStatsColumns["errors"]).Value,

					LastIndexedEtag =
						Api.RetrieveColumn(session, IndexesStats, tableColumnsCache.IndexesStatsColumns["last_indexed_etag"]).TransfromToGuidWithProperSorting(),
					LastIndexedTimestamp =
						Api.RetrieveColumnAsDateTime(session, IndexesStats, tableColumnsCache.IndexesStatsColumns["last_indexed_timestamp"]).Value,
						
					ReduceIndexingAttempts = hasReduce == false ? null : Api.RetrieveColumnAsInt32(session, IndexesStatsReduce, tableColumnsCache.IndexesStatsReduceColumns["reduce_attempts"]),
					ReduceIndexingSuccesses = hasReduce == false ? null : 
						Api.RetrieveColumnAsInt32(session, IndexesStatsReduce, tableColumnsCache.IndexesStatsReduceColumns["reduce_successes"]),
					ReduceIndexingErrors = hasReduce == false ? null : 
						Api.RetrieveColumnAsInt32(session, IndexesStatsReduce, tableColumnsCache.IndexesStatsReduceColumns["reduce_errors"]),
					LastReducedEtag = hasReduce == false ? (Guid?)null : GetLastReduceIndexWithPotentialNull(),
					LastReducedTimestamp = hasReduce == false ? (DateTime?)null : GetLastReducedTimestampWithPotentialNull(),
				
				};
			}
		}

		private DateTime GetLastReducedTimestampWithPotentialNull()
		{
			return Api.RetrieveColumnAsDateTime(session, IndexesStatsReduce, tableColumnsCache.IndexesStatsReduceColumns["last_reduced_timestamp"]) ?? DateTime.MinValue;
		}

		private Guid GetLastReduceIndexWithPotentialNull()
		{
			var bytes = Api.RetrieveColumn(session, IndexesStatsReduce, tableColumnsCache.IndexesStatsReduceColumns["last_reduced_etag"]);
			if(bytes == null)
				return Guid.Empty;
			return bytes.TransfromToGuidWithProperSorting();
		}

		public void AddIndex(string name, bool createMapReduce)
		{
			using (var update = new Update(session, IndexesStats, JET_prep.Insert))
			{
				Api.SetColumn(session, IndexesStats, tableColumnsCache.IndexesStatsColumns["key"], name, Encoding.Unicode);
				Api.SetColumn(session, IndexesStats, tableColumnsCache.IndexesStatsColumns["last_indexed_etag"], Guid.Empty.TransformToValueForEsentSorting());
				Api.SetColumn(session, IndexesStats, tableColumnsCache.IndexesStatsColumns["last_indexed_timestamp"], DateTime.MinValue);
				update.Save();
			}


			using (var update = new Update(session, IndexesEtags, JET_prep.Insert))
			{
				Api.SetColumn(session, IndexesEtags, tableColumnsCache.IndexesEtagsColumns["key"], name, Encoding.Unicode);
				update.Save();
			}

			if (createMapReduce == false)
				return;

			using (var update = new Update(session, IndexesStatsReduce, JET_prep.Insert))
			{
				Api.SetColumn(session, IndexesStatsReduce, tableColumnsCache.IndexesStatsReduceColumns["key"], name, Encoding.Unicode);
				Api.SetColumn(session, IndexesStatsReduce, tableColumnsCache.IndexesStatsReduceColumns["last_reduced_etag"], Guid.Empty.TransformToValueForEsentSorting());
				Api.SetColumn(session, IndexesStatsReduce, tableColumnsCache.IndexesStatsReduceColumns["last_reduced_timestamp"], DateTime.MinValue);
				update.Save();
			}
		}

		public void DeleteIndex(string name)
		{
			Api.JetSetCurrentIndex(session, IndexesStats, "by_key");
			Api.MakeKey(session, IndexesStats, name, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, IndexesStats, SeekGrbit.SeekEQ))
			{
				Api.JetDelete(session, IndexesStats);
			}

			Api.JetSetCurrentIndex(session, IndexesEtags, "by_key");
			Api.MakeKey(session, IndexesEtags, name, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, IndexesEtags, SeekGrbit.SeekEQ))
			{
				Api.JetDelete(session, IndexesEtags);
			}

			Api.JetSetCurrentIndex(session, IndexesStatsReduce, "by_key");
			Api.MakeKey(session, IndexesStatsReduce, name, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, IndexesStatsReduce, SeekGrbit.SeekEQ))
			{
				Api.JetDelete(session, IndexesStatsReduce);
			}

			foreach (var table in new[]{MappedResults, ReducedResults, ScheduledReductions})
			{
				Api.JetSetCurrentIndex(session, table, "by_view");
				Api.MakeKey(session, table, name, Encoding.Unicode, MakeKeyGrbit.NewKey);
				if (!Api.TrySeek(session, table, SeekGrbit.SeekEQ))
					continue;

				Api.MakeKey(session, table, name, Encoding.Unicode, MakeKeyGrbit.NewKey);
				Api.JetSetIndexRange(session, table, SetIndexRangeGrbit.RangeInclusive | SetIndexRangeGrbit.RangeUpperLimit);
				var count = 0;
				do
				{
					if(count++ > 1000)
					{
						PulseTransaction();
						count = 0;
					}
						
					Api.JetDelete(session, table);
				} while (Api.TryMoveNext(session, table));
			}

		}

		public IndexFailureInformation GetFailureRate(string index)
		{
			var hasReduce = SetCurrentIndexStatsToImpl(index);
			return new IndexFailureInformation
			{
				Name = index,
				Attempts = Api.RetrieveColumnAsInt32(session, IndexesStats, tableColumnsCache.IndexesStatsColumns["attempts"]).Value,
				Errors = Api.RetrieveColumnAsInt32(session, IndexesStats, tableColumnsCache.IndexesStatsColumns["errors"]).Value,
				Successes = Api.RetrieveColumnAsInt32(session, IndexesStats, tableColumnsCache.IndexesStatsColumns["successes"]).Value,
				ReduceAttempts = hasReduce ? Api.RetrieveColumnAsInt32(session, IndexesStatsReduce, tableColumnsCache.IndexesStatsReduceColumns["reduce_attempts"]): null,
				ReduceErrors = hasReduce ? Api.RetrieveColumnAsInt32(session, IndexesStatsReduce, tableColumnsCache.IndexesStatsReduceColumns["reduce_errors"]) : null,
				ReduceSuccesses = hasReduce ? Api.RetrieveColumnAsInt32(session, IndexesStatsReduce, tableColumnsCache.IndexesStatsReduceColumns["reduce_successes"]) : null
			};
		}

		public void UpdateLastIndexed(string index, Guid etag, DateTime timestamp)
		{
			Api.JetSetCurrentIndex(session, IndexesStats, "by_key");
			Api.MakeKey(session, IndexesStats, index, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, IndexesStats, SeekGrbit.SeekEQ) == false)
				throw new IndexDoesNotExistsException("There is no index named: " + index);

			using(var update = new Update(session, IndexesStats, JET_prep.Replace))
			{
				Api.SetColumn(session, IndexesStats,
				              tableColumnsCache.IndexesStatsColumns["last_indexed_etag"],
				              etag.TransformToValueForEsentSorting());
				Api.SetColumn(session, IndexesStats,
							  tableColumnsCache.IndexesStatsColumns["last_indexed_timestamp"],
							  timestamp);
				update.Save();
			}
		}

		public void UpdateIndexingStats(string index, IndexingWorkStats stats)
		{
			SetCurrentIndexStatsToImpl(index);
			using(var update = new Update(session, IndexesStats, JET_prep.Replace))
			{
				var oldAttempts = Api.RetrieveColumnAsInt32(session, IndexesStats, tableColumnsCache.IndexesStatsColumns["attempts"]) ?? 0;
				Api.SetColumn(session,IndexesStats, tableColumnsCache.IndexesStatsColumns["attempts"], 
					oldAttempts + stats.IndexingAttempts);

				var oldErrors = Api.RetrieveColumnAsInt32(session, IndexesStats, tableColumnsCache.IndexesStatsColumns["errors"]) ?? 0;
				Api.SetColumn(session,IndexesStats, tableColumnsCache.IndexesStatsColumns["errors"], 
					oldErrors + stats.IndexingErrors);

				var olsSuccesses = Api.RetrieveColumnAsInt32(session, IndexesStats, tableColumnsCache.IndexesStatsColumns["successes"]) ?? 0;
				Api.SetColumn(session,IndexesStats, tableColumnsCache.IndexesStatsColumns["successes"], 
					olsSuccesses + stats.IndexingSuccesses);

				update.Save();
			}
		}

		public void UpdateReduceStats(string index, IndexingWorkStats stats)
		{
			SetCurrentIndexStatsToImpl(index);
			using (var update = new Update(session, IndexesStatsReduce, JET_prep.Replace))
			{
				var oldAttempts = Api.RetrieveColumnAsInt32(session, IndexesStatsReduce, tableColumnsCache.IndexesStatsReduceColumns["reduce_attempts"]) ?? 0;
				Api.SetColumn(session, IndexesStatsReduce, tableColumnsCache.IndexesStatsReduceColumns["reduce_attempts"],
					oldAttempts + stats.ReduceAttempts);

				var oldErrors = Api.RetrieveColumnAsInt32(session, IndexesStatsReduce, tableColumnsCache.IndexesStatsReduceColumns["reduce_errors"]) ?? 0;
				Api.SetColumn(session, IndexesStatsReduce, tableColumnsCache.IndexesStatsReduceColumns["reduce_errors"],
					oldErrors + stats.ReduceErrors);

				var olsSuccesses = Api.RetrieveColumnAsInt32(session, IndexesStatsReduce, tableColumnsCache.IndexesStatsReduceColumns["reduce_successes"]) ?? 0;
				Api.SetColumn(session, IndexesStatsReduce, tableColumnsCache.IndexesStatsReduceColumns["reduce_successes"],
					olsSuccesses + stats.ReduceSuccesses);

				update.Save();
			}
		}

		public void TouchIndexEtag(string index)
		{
			Api.JetSetCurrentIndex(session, IndexesEtags, "by_key");
			Api.MakeKey(session, IndexesEtags, index, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, IndexesEtags, SeekGrbit.SeekEQ) == false)
				throw new IndexDoesNotExistsException("There is no reduce index named: " + index);

			Api.EscrowUpdate(session, IndexesEtags, tableColumnsCache.IndexesEtagsColumns["touches"], 1);
		}

		public void UpdateLastReduced(string index, Guid etag, DateTime timestamp)
		{
			Api.JetSetCurrentIndex(session, IndexesStatsReduce, "by_key");
			Api.MakeKey(session, IndexesStatsReduce, index, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, IndexesStatsReduce, SeekGrbit.SeekEQ) == false)
				throw new IndexDoesNotExistsException("There is no reduce index named: " + index);

			using (var update = new Update(session, IndexesStatsReduce, JET_prep.Replace))
			{
				Api.SetColumn(session, IndexesStatsReduce,
							  tableColumnsCache.IndexesStatsReduceColumns["last_reduced_etag"],
							  etag.TransformToValueForEsentSorting());
				Api.SetColumn(session, IndexesStatsReduce,
							  tableColumnsCache.IndexesStatsReduceColumns["last_reduced_timestamp"],
							  timestamp);
				update.Save();
			}
		}


	}
}

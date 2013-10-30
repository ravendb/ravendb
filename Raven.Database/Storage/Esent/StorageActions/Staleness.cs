//-----------------------------------------------------------------------
// <copyright file="Staleness.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Isam.Esent.Interop;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Exceptions;
using Raven.Database.Impl;
using Raven.Database.Impl.Synchronization;
using Raven.Database.Json;
using Raven.Database.Storage;
using Raven.Database.Extensions;
using System.Linq;
using Raven.Json.Linq;

namespace Raven.Storage.Esent.StorageActions
{
	public partial class DocumentStorageActions : IStalenessStorageActions
	{
		public bool IsIndexStale(int view, DateTime? cutOff, Etag cutoffEtag)
		{
			Api.JetSetCurrentIndex(session, IndexesStats, "by_key");
			Api.MakeKey(session, IndexesStats, view, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, IndexesStats, SeekGrbit.SeekEQ) == false)
			{
				return false;
			}

			Api.JetSetCurrentIndex(session, IndexesStatsReduce, "by_key");
			Api.MakeKey(session, IndexesStatsReduce, view, MakeKeyGrbit.NewKey);
			var hasReduce = Api.TrySeek(session, IndexesStatsReduce, SeekGrbit.SeekEQ);

			if (IsMapStale(view) || hasReduce && IsReduceStale(view))
			{
				if (cutOff != null)
				{
					var indexedTimestamp = Api.RetrieveColumnAsInt64(session, IndexesStats, tableColumnsCache.IndexesStatsColumns["last_indexed_timestamp"]).Value;
					var lastIndexedTimestamp = DateTime.FromBinary(indexedTimestamp);
					if (cutOff.Value >= lastIndexedTimestamp)
						return true;

					if (hasReduce)
					{
						var lastReduceIndex = Api.RetrieveColumnAsInt64(session, IndexesStatsReduce, tableColumnsCache.IndexesStatsReduceColumns["last_reduced_timestamp"]);
						lastIndexedTimestamp = lastReduceIndex == null ? DateTime.MinValue : DateTime.FromBinary(lastReduceIndex.Value);
						if (cutOff.Value >= lastIndexedTimestamp)
							return true;
					}
				}
				else if (cutoffEtag != null)
				{
					var lastIndexedEtag = Api.RetrieveColumn(session, IndexesStats,
												  tableColumnsCache.IndexesStatsColumns["last_indexed_etag"]);

					if (Buffers.Compare(lastIndexedEtag, cutoffEtag.ToByteArray()) < 0)
						return true;
				}
				else
				{
					return true;
				}
			}

			Api.JetSetCurrentIndex(session, Tasks, "by_index");
			Api.MakeKey(session, Tasks, view, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, Tasks, SeekGrbit.SeekEQ) == false)
			{
				return false;
			}
			if (cutOff == null)
				return true;
			// we are at the first row for this index
			var addedAt = Api.RetrieveColumnAsInt64(session, Tasks, tableColumnsCache.TasksColumns["added_at"]).Value;
			return cutOff.Value >= DateTime.FromBinary(addedAt);
		}

		public bool IsReduceStale(int view)
		{
			Api.JetSetCurrentIndex(session, ScheduledReductions, "by_view_level_and_hashed_reduce_key_and_bucket");
			Api.MakeKey(session, ScheduledReductions, view, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, ScheduledReductions, SeekGrbit.SeekGE) == false)
				return false;
			var dbView = Api.RetrieveColumnAsInt32(session, ScheduledReductions, tableColumnsCache.ScheduledReductionColumns["view"], RetrieveColumnGrbit.RetrieveFromIndex);
      return dbView == view;
		}

		public bool IsMapStale(int view)
		{
			Api.JetSetCurrentIndex(session, IndexesStats, "by_key");
			Api.MakeKey(session, IndexesStats, view, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, IndexesStats, SeekGrbit.SeekEQ) == false)
				return false;

			var lastIndexedEtag = Api.RetrieveColumn(session, IndexesStats,
													 tableColumnsCache.IndexesStatsColumns["last_indexed_etag"]);
			Api.JetSetCurrentIndex(session, Documents, "by_etag");
			if (!Api.TryMoveLast(session, Documents))
			{
				return false;
			}
			var lastEtag = Api.RetrieveColumn(session, Documents, tableColumnsCache.DocumentsColumns["etag"]);
			return (Buffers.Compare(lastEtag, lastIndexedEtag) > 0);
		}

		public Tuple<DateTime, Etag> IndexLastUpdatedAt(int view)
		{
			Api.JetSetCurrentIndex(session, IndexesStats, "by_key");
			Api.MakeKey(session, IndexesStats, view, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, IndexesStats, SeekGrbit.SeekEQ) == false)
			{
				throw new IndexDoesNotExistsException("Could not find index named: " + view);
			}

			Api.JetSetCurrentIndex(session, IndexesStatsReduce, "by_key");
			Api.MakeKey(session, IndexesStatsReduce, view, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, IndexesStatsReduce, SeekGrbit.SeekEQ))
			{// for map-reduce indexes, we use the reduce stats

				var binary = Api.RetrieveColumnAsInt64(session, IndexesStatsReduce, tableColumnsCache.IndexesStatsReduceColumns["last_reduced_timestamp"]);
				var lastReducedIndex = binary == null ? DateTime.MinValue : DateTime.FromBinary(binary.Value);
				var lastReducedEtag = Etag.Parse(Api.RetrieveColumn(session, IndexesStatsReduce,
																		  tableColumnsCache.IndexesStatsReduceColumns["last_reduced_etag"]));
				return Tuple.Create(lastReducedIndex, lastReducedEtag);

			}


			var indexedTimestamp = Api.RetrieveColumnAsInt64(session, IndexesStats, tableColumnsCache.IndexesStatsColumns["last_indexed_timestamp"]).Value;
			var lastIndexedTimestamp = DateTime.FromBinary(indexedTimestamp);
			var lastIndexedEtag = Etag.Parse(Api.RetrieveColumn(session, IndexesStats, tableColumnsCache.IndexesStatsColumns["last_indexed_etag"]));
			return Tuple.Create(lastIndexedTimestamp, lastIndexedEtag);
		}

		public Etag GetMostRecentDocumentEtag()
		{
			Api.JetSetCurrentIndex(session, Documents, "by_etag");
			if (!Api.TryMoveLast(session, Documents))
			{
				return Etag.Empty;
			}
			var lastEtag = Api.RetrieveColumn(session, Documents, tableColumnsCache.DocumentsColumns["etag"]);
			return Etag.Parse(lastEtag);
		}

		public Etag GetMostRecentAttachmentEtag()
		{
			Api.JetSetCurrentIndex(session, Files, "by_etag");
			if (!Api.TryMoveLast(session, Files))
			{
				return Etag.Empty;
			}
			var lastEtag = Api.RetrieveColumn(session, Files, tableColumnsCache.DocumentsColumns["etag"]);
			return Etag.Parse(lastEtag);
		}

		public int GetIndexTouchCount(int view)
		{
			Api.JetSetCurrentIndex(session, IndexesEtags, "by_key");
			Api.MakeKey(session, IndexesEtags, view, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, IndexesEtags, SeekGrbit.SeekEQ) == false) // find the next greater view
				return -1;

			return Api.RetrieveColumnAsInt32(session, IndexesEtags, tableColumnsCache.IndexesEtagsColumns["touches"]).Value;
		}

	}
}

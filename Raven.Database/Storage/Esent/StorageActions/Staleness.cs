//-----------------------------------------------------------------------
// <copyright file="Staleness.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Text;
using Microsoft.Isam.Esent.Interop;
using Raven.Abstractions.Extensions;
using Raven.Database.Exceptions;
using Raven.Database.Json;
using Raven.Database.Storage;
using Raven.Database.Extensions;
using System.Linq;

namespace Raven.Storage.Esent.StorageActions
{
	public partial class DocumentStorageActions : IStalenessStorageActions
	{
		public bool IsIndexStale(string name, DateTime? cutOff, Guid? cutoffEtag)
		{
			Api.JetSetCurrentIndex(session, IndexesStats, "by_key");
			Api.MakeKey(session, IndexesStats, name, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, IndexesStats, SeekGrbit.SeekEQ) == false)
			{
				return false;
			}

			Api.JetSetCurrentIndex(session, IndexesStatsReduce, "by_key");
			Api.MakeKey(session, IndexesStatsReduce, name, Encoding.Unicode, MakeKeyGrbit.NewKey);
			var hasReduce = Api.TrySeek(session, IndexesStatsReduce, SeekGrbit.SeekEQ);

			if (IsMapStale(name) || hasReduce && IsReduceStale(name))
			{
				if (cutOff != null)
				{
					var lastIndexedTimestamp =
						Api.RetrieveColumnAsDateTime(session, IndexesStats,
													 tableColumnsCache.IndexesStatsColumns["last_indexed_timestamp"])
							.Value;
					if (cutOff.Value >= lastIndexedTimestamp)
						return true;

					if (hasReduce)
					{
						lastIndexedTimestamp =
							Api.RetrieveColumnAsDateTime(session, IndexesStatsReduce,
							                             tableColumnsCache.IndexesStatsReduceColumns["last_reduced_timestamp"]) ??
							DateTime.MinValue;
						if (cutOff.Value >= lastIndexedTimestamp)
							return true;
					}
				}
				else if(cutoffEtag != null)
				{
					var lastIndexedEtag = Api.RetrieveColumn(session, IndexesStats,
												  tableColumnsCache.IndexesStatsColumns["last_indexed_etag"]);

					if (Buffers.Compare(lastIndexedEtag, cutoffEtag.Value.ToByteArray()) < 0)
						return true;
				}
				else
				{
					return true;
				}
			}

			Api.JetSetCurrentIndex(session, Tasks, "by_index");
			Api.MakeKey(session, Tasks, name, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, Tasks, SeekGrbit.SeekEQ) == false)
			{
				return false;
			}
			if (cutOff == null)
				return true;
			// we are at the first row for this index
			var addedAt = Api.RetrieveColumnAsDateTime(session, Tasks, tableColumnsCache.TasksColumns["added_at"]).Value;
			return cutOff.Value >= addedAt;
		}

		public bool IsReduceStale(string name)
		{
			Api.JetSetCurrentIndex(session, ScheduledReductions, "by_view");
			Api.MakeKey(session, ScheduledReductions, name, Encoding.Unicode, MakeKeyGrbit.NewKey);
			return Api.TrySeek(session, ScheduledReductions, SeekGrbit.SeekEQ);
		}

		public bool IsMapStale(string name)
		{
			 Api.JetSetCurrentIndex(session, IndexesStats, "by_key");
			Api.MakeKey(session, IndexesStats, name, Encoding.Unicode, MakeKeyGrbit.NewKey);
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
			return Buffers.Compare(lastEtag, lastIndexedEtag) > 0;
		}

		public Tuple<DateTime, Guid> IndexLastUpdatedAt(string name)
		{
			Api.JetSetCurrentIndex(session, IndexesStats, "by_key");
			Api.MakeKey(session, IndexesStats, name, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, IndexesStats, SeekGrbit.SeekEQ) == false)
			{
				throw new IndexDoesNotExistsException("Could not find index named: " + name);
			}

			Api.JetSetCurrentIndex(session, IndexesStatsReduce, "by_key");
			Api.MakeKey(session, IndexesStatsReduce, name, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if(Api.TrySeek(session, IndexesStatsReduce, SeekGrbit.SeekEQ)) 
			{// for map-reduce indexes, we use the reduce stats

				var retrieveColumnAsDateTime = Api.RetrieveColumnAsDateTime(session, IndexesStatsReduce, tableColumnsCache.IndexesStatsReduceColumns["last_reduced_timestamp"]) ?? DateTime.MinValue;
				var lastReducedIndex = retrieveColumnAsDateTime;
				var lastReducedEtag = Api.RetrieveColumn(session, IndexesStatsReduce,
																		  tableColumnsCache.IndexesStatsReduceColumns["last_reduced_etag"]).TransfromToGuidWithProperSorting();
				return Tuple.Create(lastReducedIndex, lastReducedEtag);
	
			}
	    

			var lastIndexedTimestamp = Api.RetrieveColumnAsDateTime(session, IndexesStats,
																  tableColumnsCache.IndexesStatsColumns["last_indexed_timestamp"])
				.Value;
			var lastIndexedEtag = Api.RetrieveColumn(session, IndexesStats,
																	  tableColumnsCache.IndexesStatsColumns["last_indexed_etag"]).TransfromToGuidWithProperSorting();
			return Tuple.Create(lastIndexedTimestamp, lastIndexedEtag);
		}

		public Guid GetMostRecentDocumentEtag()
		{
			Api.JetSetCurrentIndex(session, Documents, "by_etag");
			if (!Api.TryMoveLast(session, Documents))
			{
				return Guid.Empty;
			}
			var lastEtag = Api.RetrieveColumn(session, Documents, tableColumnsCache.DocumentsColumns["etag"]);
			return new Guid(lastEtag);
		}

		public Guid GetMostRecentAttachmentEtag()
		{
			Api.JetSetCurrentIndex(session, Files, "by_etag");
			if (!Api.TryMoveLast(session, Files))
			{
				return Guid.Empty;
			}
			var lastEtag = Api.RetrieveColumn(session, Files, tableColumnsCache.DocumentsColumns["etag"]);
			return new Guid(lastEtag);
		}

		public int GetIndexTouchCount(string name)
		{
			Api.JetSetCurrentIndex(session, IndexesEtags, "by_key");
			Api.MakeKey(session, IndexesEtags, name, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, IndexesEtags, SeekGrbit.SeekEQ) == false) // find the next greater view
				return -1;

			return Api.RetrieveColumnAsInt32(session, IndexesEtags, tableColumnsCache.IndexesEtagsColumns["touches"]).Value;
		}


		public Guid? GetMostRecentReducedEtag(string name)
		{
			Api.JetSetCurrentIndex(session, MappedResults, "by_view_and_etag");
			Api.MakeKey(session, MappedResults, name, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if(Api.TrySeek(session, MappedResults, SeekGrbit.SeekGE) == false) // find the next greater view
				return null;

			// did we find the last item on the view?
			if (Api.RetrieveColumnAsString(session, MappedResults, tableColumnsCache.MappedResultsColumns["view"], Encoding.Unicode) == name)
				return new Guid(Api.RetrieveColumn(session, MappedResults, tableColumnsCache.MappedResultsColumns["etag"]));

			// maybe we are at another view?
			if (Api.TryMovePrevious(session, MappedResults) == false) // move one step back, now we are at the highest etag for this view, maybe
				return null;

			//could't find the name in the table 
			if(Api.RetrieveColumnAsString(session, MappedResults, tableColumnsCache.MappedResultsColumns["view"],Encoding.Unicode) != name)
				return null;

			return new Guid(Api.RetrieveColumn(session, MappedResults, tableColumnsCache.MappedResultsColumns["etag"]));
		}

		

	}
}
//-----------------------------------------------------------------------
// <copyright file="Staleness.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Text;
using Microsoft.Isam.Esent.Interop;
using Raven.Database.Exceptions;
using Raven.Database.Json;
using Raven.Database.Storage;
using Raven.Database.Extensions;
using System.Linq;

namespace Raven.Storage.Esent.StorageActions
{
    public partial class DocumentStorageActions : IStalenessStorageActions
    {
        public bool IsIndexStale(string name, DateTime? cutOff, string entityName)
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

            if (IsStaleByEtag(name))
            {
                if (cutOff != null)
                {
                    var lastIndexedTimestamp =
                        Api.RetrieveColumnAsDateTime(session, IndexesStats,
                                                     tableColumnsCache.IndexesStatsColumns["last_indexed_timestamp"])
                            .Value;
                    if (cutOff.Value >= lastIndexedTimestamp)
                        return true;

                    lastIndexedTimestamp =
                       Api.RetrieveColumnAsDateTime(session, IndexesStatsReduce,
                                                    tableColumnsCache.IndexesStatsReduceColumns["last_reduced_timestamp"])
                           .Value;
                    if (hasReduce && cutOff.Value >= lastIndexedTimestamp)
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

        public Tuple<DateTime, Guid> IndexLastUpdatedAt(string name)
        {
            Api.JetSetCurrentIndex(session, IndexesStats, "by_key");
            Api.MakeKey(session, IndexesStats, name, Encoding.Unicode, MakeKeyGrbit.NewKey);
            if (Api.TrySeek(session, IndexesStats, SeekGrbit.SeekEQ) == false)
            {
                throw new IndexDoesNotExistsException("Could not find index named: " + name);
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

        public Guid GetMostRecentReducedEtag(string name)
        {
            Api.JetSetCurrentIndex(session, MappedResults, "by_view_and_etag");
            Api.MakeKey(session, MappedResults, name, Encoding.Unicode, MakeKeyGrbit.NewKey);
            if(Api.TrySeek(session, MappedResults, SeekGrbit.SeekGE) == false) // find the next greater view
                return Guid.Empty;

            // did we find the last item on the view?
            if (Api.RetrieveColumnAsString(session, MappedResults, tableColumnsCache.MappedResultsColumns["view"], Encoding.Unicode) == name)
                return new Guid(Api.RetrieveColumn(session, MappedResults, tableColumnsCache.MappedResultsColumns["etag"]));

            // maybe we are at another view?
            if (Api.TryMovePrevious(session, MappedResults) == false) // move one step back, now we are at the highest etag for this view, maybe
                return Guid.Empty;

            //could't find the name in the table 
            if(Api.RetrieveColumnAsString(session, MappedResults, tableColumnsCache.MappedResultsColumns["view"],Encoding.Unicode) != name)
                return Guid.Empty;

            return new Guid(Api.RetrieveColumn(session, MappedResults, tableColumnsCache.MappedResultsColumns["etag"]));
        }

        private bool IsStaleByEtag(string name)
        {
        	var lastIndexedEtag = Api.RetrieveColumn(session, IndexesStats,
        	                                         tableColumnsCache.IndexesStatsColumns["last_indexed_etag"]);
        	Api.JetSetCurrentIndex(session, Documents, "by_etag");
        	if (!Api.TryMoveLast(session, Documents))
        	{
        		return false;
        	}
        	var lastEtag = Api.RetrieveColumn(session, Documents, tableColumnsCache.DocumentsColumns["etag"]);
            if (CompareArrays(lastEtag, lastIndexedEtag) > 0)
                return true;

            var lastReducedEtag = Api.RetrieveColumn(session, IndexesStatsReduce,
                tableColumnsCache.IndexesStatsReduceColumns["last_reduced_etag"]);

            if (lastIndexedEtag.All(x => x == 0))// Guid.Empty
                return false;

            return CompareArrays(lastReducedEtag, GetMostRecentReducedEtag(name).ToByteArray()) > 0;
        }

    	private static int CompareArrays(byte[] docEtagBinary, byte[] indexEtagBinary)
        {
            for (int i = 0; i < docEtagBinary.Length; i++)
            {
                if (docEtagBinary[i].CompareTo(indexEtagBinary[i]) != 0)
                {
                    return docEtagBinary[i].CompareTo(indexEtagBinary[i]);
                }
            }
            return 0;
        }

    }
}
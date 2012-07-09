//-----------------------------------------------------------------------
// <copyright file="MappedResults.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Microsoft.Isam.Esent.Interop;
using Raven.Abstractions;
using Raven.Abstractions.Extensions;
using Raven.Database.Extensions;
using Raven.Database.Storage;
using Raven.Json.Linq;
using System.Linq;

namespace Raven.Storage.Esent.StorageActions
{
	public partial class DocumentStorageActions : IMappedResultsStorageAction
	{
		public void PutMappedResult(string view, string docId, string reduceKey, RavenJObject data, byte[] viewAndReduceKeyHashed)
		{
	        Guid etag = uuidGenerator.CreateSequentialUuid();

			using (var update = new Update(session, MappedResults, JET_prep.Insert))
			{
				Api.SetColumn(session, MappedResults, tableColumnsCache.MappedResultsColumns["view"], view, Encoding.Unicode);
				Api.SetColumn(session, MappedResults, tableColumnsCache.MappedResultsColumns["document_key"], docId, Encoding.Unicode);
				Api.SetColumn(session, MappedResults, tableColumnsCache.MappedResultsColumns["reduce_key"], reduceKey, Encoding.Unicode);
				Api.SetColumn(session, MappedResults, tableColumnsCache.MappedResultsColumns["reduce_key_and_view_hashed"], viewAndReduceKeyHashed);

				using (var stream = new BufferedStream(new ColumnStream(session, MappedResults, tableColumnsCache.MappedResultsColumns["data"])))
				{
					data.WriteTo(stream);
					stream.Flush();
				}

				Api.SetColumn(session, MappedResults, tableColumnsCache.MappedResultsColumns["etag"], etag.TransformToValueForEsentSorting());
				Api.SetColumn(session, MappedResults, tableColumnsCache.MappedResultsColumns["timestamp"], SystemTime.Now);

				update.Save();
			}
		}

		public IEnumerable<RavenJObject> GetMappedResults(params GetMappedResultsParams[] getMappedResultsParams)
		{
			var optimizedIndexReader = new OptimizedIndexReader<GetMappedResultsParams>(Session, MappedResults, getMappedResultsParams.Length);
			
			Api.JetSetCurrentIndex(session, MappedResults, "by_reduce_key_and_view_hashed");
			
			foreach (var item in getMappedResultsParams)
			{
				Api.MakeKey(session, MappedResults, item.ViewAndReduceKeyHashed, MakeKeyGrbit.NewKey);
				if (Api.TrySeek(session, MappedResults, SeekGrbit.SeekEQ) == false)
					continue;

				Api.MakeKey(session, MappedResults, item.ViewAndReduceKeyHashed, MakeKeyGrbit.NewKey);
				Api.JetSetIndexRange(session, MappedResults, SetIndexRangeGrbit.RangeUpperLimit | SetIndexRangeGrbit.RangeInclusive);
				do
				{
					optimizedIndexReader.Add(item);
				} while (Api.TryMoveNext(session, MappedResults));
			}

			return optimizedIndexReader
				.Where(item =>
				{
					// we need to check that we don't have hash collisions
					var currentReduceKey = Api.RetrieveColumnAsString(session, MappedResults,
					                                                  tableColumnsCache.MappedResultsColumns["reduce_key"]);
					if (currentReduceKey != item.ReduceKey)
						return false;

					var currentView = Api.RetrieveColumnAsString(session, MappedResults, tableColumnsCache.MappedResultsColumns["view"]);
					
					return currentView == item.View;
				})
				.Select(item =>
				{
					using (var stream = new BufferedStream(new ColumnStream(session, MappedResults, tableColumnsCache.MappedResultsColumns["data"])))
					{
						return stream.ToJObject();
					}
				});
		}

		public IEnumerable<string> DeleteMappedResultsForDocumentId(string documentId, string view)
		{
			Api.JetSetCurrentIndex(session, MappedResults, "by_view_and_doc_key");
			Api.MakeKey(session, MappedResults, view, Encoding.Unicode, MakeKeyGrbit.NewKey);
			Api.MakeKey(session, MappedResults, documentId, Encoding.Unicode, MakeKeyGrbit.None);
			if (Api.TrySeek(session, MappedResults, SeekGrbit.SeekEQ) == false)
				return new string[0];

			var reduceKeys = new HashSet<string>();
			Api.MakeKey(session, MappedResults, view, Encoding.Unicode, MakeKeyGrbit.NewKey);
			Api.MakeKey(session, MappedResults, documentId, Encoding.Unicode, MakeKeyGrbit.None);
			Api.JetSetIndexRange(session, MappedResults, SetIndexRangeGrbit.RangeUpperLimit | SetIndexRangeGrbit.RangeInclusive);
			do
			{
				// esent index ranges are approximate, and we need to check them ourselves as well
				var viewFromDb = Api.RetrieveColumnAsString(session, MappedResults, tableColumnsCache.MappedResultsColumns["view"]);
				if (StringComparer.InvariantCultureIgnoreCase.Equals(viewFromDb, view) == false)
					continue;
				var documentIdFromDb = Api.RetrieveColumnAsString(session, MappedResults, tableColumnsCache.MappedResultsColumns["document_key"]);
				if (StringComparer.InvariantCultureIgnoreCase.Equals(documentIdFromDb, documentId) == false)
					continue; 
				var reduceKey = Api.RetrieveColumnAsString(session, MappedResults, tableColumnsCache.MappedResultsColumns["reduce_key"],
														   Encoding.Unicode);
				reduceKeys.Add(reduceKey);
				Api.JetDelete(session, MappedResults);
			} while (Api.TryMoveNext(session, MappedResults));
			return reduceKeys;
		}

		public void DeleteMappedResultsForView(string view)
		{
			Api.JetSetCurrentIndex(session, MappedResults, "by_view");
			Api.MakeKey(session, MappedResults, view, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, MappedResults, SeekGrbit.SeekEQ) == false)
				return;
			Api.MakeKey(session, MappedResults, view, Encoding.Unicode, MakeKeyGrbit.NewKey);
			Api.JetSetIndexRange(session, MappedResults, SetIndexRangeGrbit.RangeUpperLimit | SetIndexRangeGrbit.RangeInclusive);

			do
			{
				// esent index ranges are approximate, and we need to check them ourselves as well
				var viewFromDb = Api.RetrieveColumnAsString(session, MappedResults, tableColumnsCache.MappedResultsColumns["view"]);
				if (StringComparer.InvariantCultureIgnoreCase.Equals(viewFromDb, view) == false)
					continue;
				Api.JetDelete(session, MappedResults);
			} while (Api.TryMoveNext(session, MappedResults));
		}

	    public IEnumerable<MappedResultInfo> GetMappedResultsReduceKeysAfter(string indexName, Guid lastReducedEtag, bool loadData, int take)
	    {
			Api.JetSetCurrentIndex(session, MappedResults, "by_view_and_etag");
			Api.MakeKey(session, MappedResults, indexName, Encoding.Unicode, MakeKeyGrbit.NewKey);
			Api.MakeKey(session, MappedResults, lastReducedEtag, MakeKeyGrbit.None);
			if (Api.TrySeek(session, MappedResults, SeekGrbit.SeekLE) == false)
				return Enumerable.Empty<MappedResultInfo>();

			var results = new Dictionary<string, MappedResultInfo>();
	        while (
				results.Count < take && 
				Api.RetrieveColumnAsString(session, MappedResults, tableColumnsCache.MappedResultsColumns["view"], Encoding.Unicode, RetrieveColumnGrbit.RetrieveFromIndex) == indexName)
	        {
	        	var mappedResultInfo = new MappedResultInfo
	        	{
	        		ReduceKey =
	        			Api.RetrieveColumnAsString(session, MappedResults, tableColumnsCache.MappedResultsColumns["reduce_key"]),
	        		Etag = new Guid(Api.RetrieveColumn(session, MappedResults, tableColumnsCache.MappedResultsColumns["etag"])),
	        		Timestamp =
	        			Api.RetrieveColumnAsDateTime(session, MappedResults, tableColumnsCache.MappedResultsColumns["timestamp"]).
	        			Value,
	        		Data = loadData
	        		       	? Api.RetrieveColumn(session, MappedResults, tableColumnsCache.MappedResultsColumns["data"]).
	        		       	  	ToJObject()
	        		       	: null,
					Size = Api.RetrieveColumnSize(session, MappedResults, tableColumnsCache.MappedResultsColumns["data"]) ?? 0
	        	};

	        	results[mappedResultInfo.ReduceKey] = mappedResultInfo;

	        	// the index is view ascending and etag descending
				// that means that we are going backward to go up
				if (Api.TryMovePrevious(session, MappedResults) == false)
					break;
	        }

	    	return results.Values;
	    }
	}
}

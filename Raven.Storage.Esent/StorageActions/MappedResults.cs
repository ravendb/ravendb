//-----------------------------------------------------------------------
// <copyright file="MappedResults.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Isam.Esent.Interop;
using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Database.Json;
using Raven.Database.Extensions;
using Raven.Database.Storage;

namespace Raven.Storage.Esent.StorageActions
{
	public partial class DocumentStorageActions : IMappedResultsStorageAction
	{
		public void PutMappedResult(string view, string docId, string reduceKey, JObject data, byte[] viewAndReduceKeyHashed)
		{
			Api.JetSetCurrentIndex(session, MappedResults, "by_pk");
			Api.MakeKey(session, MappedResults, view, Encoding.Unicode, MakeKeyGrbit.NewKey);
			Api.MakeKey(session, MappedResults, docId, Encoding.Unicode, MakeKeyGrbit.None);
			Api.MakeKey(session, MappedResults, reduceKey, Encoding.Unicode, MakeKeyGrbit.None);
			var isUpdate = Api.TrySeek(session, MappedResults, SeekGrbit.SeekEQ);
			
	        Guid etag = uuidGenerator.CreateSequentialUuid();

			using (var update = new Update(session, MappedResults, isUpdate ? JET_prep.Replace : JET_prep.Insert))
			{
				Api.SetColumn(session, MappedResults, tableColumnsCache.MappedResultsColumns["view"], view, Encoding.Unicode);
				Api.SetColumn(session, MappedResults, tableColumnsCache.MappedResultsColumns["document_key"], docId, Encoding.Unicode);
				Api.SetColumn(session, MappedResults, tableColumnsCache.MappedResultsColumns["reduce_key"], reduceKey, Encoding.Unicode);
                Api.SetColumn(session, MappedResults, tableColumnsCache.MappedResultsColumns["reduce_key_and_view_hashed"], viewAndReduceKeyHashed);
				Api.SetColumn(session, MappedResults, tableColumnsCache.MappedResultsColumns["data"], data.ToBytes());
				Api.SetColumn(session, MappedResults, tableColumnsCache.MappedResultsColumns["etag"], etag.TransformToValueForEsentSorting());

				update.Save();
			}
		}

        public IEnumerable<JObject> GetMappedResults(params GetMappedResultsParams[] getMappedResultsParams)
		{
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
					// we need to check that we don't have hash collisions
					var currentReduceKey = Api.RetrieveColumnAsString(session, MappedResults, tableColumnsCache.MappedResultsColumns["reduce_key"]);
					if (currentReduceKey != item.ReduceKey)
						continue;
			
					var currentView = Api.RetrieveColumnAsString(session, MappedResults, tableColumnsCache.MappedResultsColumns["view"]);
					if (currentView != item.View)
						continue;
					
					yield return Api.RetrieveColumn(session, MappedResults, tableColumnsCache.MappedResultsColumns["data"]).ToJObject();
				} while (Api.TryMoveNext(session, MappedResults));
        	}
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
				if (Api.RetrieveColumnAsString(session, MappedResults, tableColumnsCache.MappedResultsColumns["view"]) != view)
					continue;
				if (Api.RetrieveColumnAsString(session, MappedResults, tableColumnsCache.MappedResultsColumns["document_key"]) != documentId)
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
				if (Api.RetrieveColumnAsString(session, MappedResults, tableColumnsCache.MappedResultsColumns["view"]) != view)
					continue;
				Api.JetDelete(session, MappedResults);
			} while (Api.TryMoveNext(session, MappedResults));
		}
	}
}

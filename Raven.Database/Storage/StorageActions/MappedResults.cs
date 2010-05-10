using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Isam.Esent.Interop;
using Newtonsoft.Json.Linq;
using Raven.Database.Json;

namespace Raven.Database.Storage.StorageActions
{
	public partial class DocumentStorageActions 
	{

		public void PutMappedResult(string view, string docId, string reduceKey, string data)
		{
			Api.JetSetCurrentIndex(session, MappedResults, "by_pk");
			Api.MakeKey(session, MappedResults, view, Encoding.Unicode, MakeKeyGrbit.NewKey);
			Api.MakeKey(session, MappedResults, docId, Encoding.Unicode, MakeKeyGrbit.None);
			Api.MakeKey(session, MappedResults, reduceKey, Encoding.Unicode, MakeKeyGrbit.None);
			var isUpdate = Api.TrySeek(session, MappedResults, SeekGrbit.SeekEQ);

			Guid etag;
			DocumentDatabase.UuidCreateSequential(out etag);

			using (var update = new Update(session, MappedResults, isUpdate ? JET_prep.Replace : JET_prep.Insert))
			{
				Api.SetColumn(session, MappedResults, tableColumnsCache.MappedResultsColumns["view"], view, Encoding.Unicode);
				Api.SetColumn(session, MappedResults, tableColumnsCache.MappedResultsColumns["document_key"], docId, Encoding.Unicode);
				Api.SetColumn(session, MappedResults, tableColumnsCache.MappedResultsColumns["reduce_key"], reduceKey, Encoding.Unicode);
				Api.SetColumn(session, MappedResults, tableColumnsCache.MappedResultsColumns["data"], Encoding.UTF8.GetBytes(data));
				Api.SetColumn(session, MappedResults, tableColumnsCache.MappedResultsColumns["etag"], etag.ToByteArray());

				update.Save();
			}
		}

		public IEnumerable<JObject> GetMappedResults(string view, string reduceKey)
		{
			Api.JetSetCurrentIndex(session, MappedResults, "by_view_and_reduce_key");
			Api.MakeKey(session, MappedResults, view, Encoding.Unicode, MakeKeyGrbit.NewKey);
			Api.MakeKey(session, MappedResults, reduceKey, Encoding.Unicode, MakeKeyGrbit.None);
			if (Api.TrySeek(session, MappedResults, SeekGrbit.SeekEQ) == false)
				yield break;

			Api.MakeKey(session, MappedResults, view, Encoding.Unicode, MakeKeyGrbit.NewKey);
			Api.MakeKey(session, MappedResults, reduceKey, Encoding.Unicode, MakeKeyGrbit.None);
			Api.JetSetIndexRange(session, MappedResults, SetIndexRangeGrbit.RangeUpperLimit | SetIndexRangeGrbit.RangeInclusive);

			do
			{
				// esent index ranges are approximate, and we need to check them ourselves as well
				if (Api.RetrieveColumnAsString(session, MappedResults, tableColumnsCache.MappedResultsColumns["reduce_key"]) != reduceKey)
					continue;
				if (Api.RetrieveColumnAsString(session, MappedResults, tableColumnsCache.MappedResultsColumns["view"]) != view)
					continue;
				yield return Api.RetrieveColumn(session, MappedResults, tableColumnsCache.MappedResultsColumns["data"]).ToJObject();
			} while (Api.TryMoveNext(session, MappedResults));
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
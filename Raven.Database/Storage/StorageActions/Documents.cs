using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Isam.Esent.Interop;
using Newtonsoft.Json.Linq;
using Raven.Database.Extensions;
using Raven.Database.Json;

namespace Raven.Database.Storage.StorageActions
{
	public partial class DocumentStorageActions 
	{

		public int GetDocumentsCount()
		{
			if (Api.TryMoveFirst(session, Details))
				return Api.RetrieveColumnAsInt32(session, Details, tableColumnsCache.DetailsColumns["document_count"]).Value;
			return 0;
		}

		public IEnumerable<string> DocumentKeys
		{
			get
			{
				Api.MoveBeforeFirst(session, Documents);
				while (Api.TryMoveNext(session, Documents))
				{
					yield return
						Api.RetrieveColumnAsString(session, Documents, tableColumnsCache.DocumentsColumns["key"], Encoding.Unicode);
				}
			}
		}



		public JsonDocument DocumentByKey(string key, TransactionInformation transactionInformation)
		{
			byte[] data;
			if (transactionInformation != null)
			{
				Api.JetSetCurrentIndex(session, DocumentsModifiedByTransactions, "by_key");
				Api.MakeKey(session, DocumentsModifiedByTransactions, key, Encoding.Unicode, MakeKeyGrbit.NewKey);
				if (Api.TrySeek(session, DocumentsModifiedByTransactions, SeekGrbit.SeekEQ))
				{
					var txId = Api.RetrieveColumn(session, DocumentsModifiedByTransactions, tableColumnsCache.DocumentsModifiedByTransactionsColumns["locked_by_transaction"]);
					if (new Guid(txId) == transactionInformation.Id)
					{
						if (Api.RetrieveColumnAsBoolean(session, DocumentsModifiedByTransactions, tableColumnsCache.DocumentsModifiedByTransactionsColumns["delete_document"]) == true)
						{
							logger.DebugFormat("Document with key '{0}' was deleted in transaction: {1}", key, transactionInformation.Id);
							return null;
						}
						data = Api.RetrieveColumn(session, DocumentsModifiedByTransactions, tableColumnsCache.DocumentsModifiedByTransactionsColumns["data"]);
						logger.DebugFormat("Document with key '{0}' was found in transaction: {1}", key, transactionInformation.Id);
						var etag = new Guid(Api.RetrieveColumn(session, DocumentsModifiedByTransactions, tableColumnsCache.DocumentsModifiedByTransactionsColumns["etag"]));
						return new JsonDocument
						{
							DataAsJson = data.ToJObject(),
							Etag = etag,
							Key = Api.RetrieveColumnAsString(session, DocumentsModifiedByTransactions, tableColumnsCache.DocumentsModifiedByTransactionsColumns["key"], Encoding.Unicode),
							Metadata = Api.RetrieveColumn(session, DocumentsModifiedByTransactions, tableColumnsCache.DocumentsModifiedByTransactionsColumns["metadata"]).ToJObject()
						};
					}
				}
			}

			Api.JetSetCurrentIndex(session, Documents, "by_key");
			Api.MakeKey(session, Documents, key, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, Documents, SeekGrbit.SeekEQ) == false)
			{
				logger.DebugFormat("Document with key '{0}' was not found", key);
				return null;
			}
			data = Api.RetrieveColumn(session, Documents, tableColumnsCache.DocumentsColumns["data"]);
			logger.DebugFormat("Document with key '{0}' was found", key);
			var existingEtag = new Guid(Api.RetrieveColumn(session, Documents, tableColumnsCache.DocumentsColumns["etag"]));
			return new JsonDocument
			{
				DataAsJson = data.ToJObject(),
				Etag = existingEtag,
				Key = Api.RetrieveColumnAsString(session, Documents, tableColumnsCache.DocumentsColumns["key"], Encoding.Unicode),
				Metadata = Api.RetrieveColumn(session, Documents, tableColumnsCache.DocumentsColumns["metadata"]).ToJObject()
			};
		}

		public IEnumerable<JsonDocument> GetDocumentsByReverseCreationOrder(Reference<bool> hasMore, int start)
		{
			Api.MoveAfterLast(session, Documents);
			for (int i = 0; i < start; i++)
			{
				if(Api.TryMovePrevious(session,Documents) == false)
					yield break;
			}
			while (Api.TryMovePrevious(session, Documents))
			{
				yield return new JsonDocument
				{
					Key = Api.RetrieveColumnAsString(session, Documents, tableColumnsCache.DocumentsColumns["key"], Encoding.Unicode),
					DataAsJson = Api.RetrieveColumn(session, Documents, tableColumnsCache.DocumentsColumns["data"]).ToJObject(),
					Etag = new Guid(Api.RetrieveColumn(session, Documents, tableColumnsCache.DocumentsColumns["etag"])),
					Metadata = Api.RetrieveColumn(session, Documents, tableColumnsCache.DocumentsColumns["metadata"]).ToJObject()
				};	
			}
		}

        public IEnumerable<JsonDocument> GetDocumentsAfter(Guid etag)
        {
            Api.JetSetCurrentIndex(session, Documents, "by_etag");
            var byteArray = etag.ToByteArray();
            Api.MakeKey(session, Documents, byteArray, MakeKeyGrbit.NewKey);
            if(Api.TrySeek(session, Documents, SeekGrbit.SeekGT)==false)
                yield break;
            do
            {
                yield return new JsonDocument
                {
                    Key =
                        Api.RetrieveColumnAsString(session, Documents, tableColumnsCache.DocumentsColumns["key"],
                                                   Encoding.Unicode),
                    DataAsJson =
                        Api.RetrieveColumn(session, Documents, tableColumnsCache.DocumentsColumns["data"]).ToJObject(),
                    Etag = new Guid(Api.RetrieveColumn(session, Documents, tableColumnsCache.DocumentsColumns["etag"])),
                    Metadata =
                        Api.RetrieveColumn(session, Documents, tableColumnsCache.DocumentsColumns["metadata"]).ToJObject
                            ()
                };
            } while (Api.TryMoveNext(session, Documents));
        }

		public IEnumerable<Tuple<JsonDocument, int>> DocumentsById(Reference<bool> hasMoreWork, int startId, int endId,
																   int limit)
		{
			Api.JetSetCurrentIndex(session, Documents, "by_id");
			Api.MakeKey(session, Documents, startId, MakeKeyGrbit.NewKey);
			// this sholdn't really happen, it means that the doc is missing
			// probably deleted before we can get it?
			if (Api.TrySeek(session, Documents, SeekGrbit.SeekGE) == false)
			{
				logger.DebugFormat("Document with id {0} or higher was not found", startId);
				yield break;
			}
			var count = 0;
			do
			{
				if ((++count) > limit)
				{
					hasMoreWork.Value = true;
					yield break;
				}
				var id = Api.RetrieveColumnAsInt32(session, Documents, tableColumnsCache.DocumentsColumns["id"],
												   RetrieveColumnGrbit.RetrieveFromIndex).Value;
				if (id > endId)
					break;

				var data = Api.RetrieveColumn(session, Documents, tableColumnsCache.DocumentsColumns["data"]);
				logger.DebugFormat("Document with id '{0}' was found, doc length: {1}", id, data.Length);
				var etag = new Guid(Api.RetrieveColumn(session, Documents, tableColumnsCache.DocumentsColumns["etag"]));
				var doc = new JsonDocument
				{
					Key = Api.RetrieveColumnAsString(session, Documents, tableColumnsCache.DocumentsColumns["key"], Encoding.Unicode),
					DataAsJson = data.ToJObject(),
					Etag = etag,
					Metadata = Api.RetrieveColumn(session, Documents, tableColumnsCache.DocumentsColumns["metadata"]).ToJObject()
				};
				yield return new Tuple<JsonDocument, int>(doc, id);
			} while (Api.TryMoveNext(session, Documents));
			hasMoreWork.Value = false;
		}

		public Guid AddDocument(string key, Guid? etag, JObject data, JObject metadata)
		{
			Api.JetSetCurrentIndex(session, Documents, "by_key");
			Api.MakeKey(session, Documents, key, Encoding.Unicode, MakeKeyGrbit.NewKey);
			var isUpdate = Api.TrySeek(session, Documents, SeekGrbit.SeekEQ);
			if (isUpdate)
			{
				EnsureNotLockedByTransaction(key, null);
				EnsureDocumentEtagMatch(key, etag, "PUT");
			}
			else
			{
				EnsureDocumentIsNotCreatedInAnotherTransaction(key, Guid.NewGuid());
				if (Api.TryMoveFirst(session, Details))
					Api.EscrowUpdate(session, Details, tableColumnsCache.DetailsColumns["document_count"], 1);
			}
		    Guid newEtag = DocumentDatabase.CreateSequentialUuid();

			using (var update = new Update(session, Documents, isUpdate ? JET_prep.Replace : JET_prep.Insert))
			{
				Api.SetColumn(session, Documents, tableColumnsCache.DocumentsColumns["key"], key, Encoding.Unicode);
				Api.SetColumn(session, Documents, tableColumnsCache.DocumentsColumns["data"], Encoding.UTF8.GetBytes(data.ToString()));
			    Api.SetColumn(session, Documents, tableColumnsCache.DocumentsColumns["etag"], newEtag.ToByteArray());
				Api.SetColumn(session, Documents, tableColumnsCache.DocumentsColumns["metadata"], Encoding.UTF8.GetBytes(metadata.ToString()));

				update.Save();
			}

			logger.DebugFormat("Inserted a new document with key '{0}', update: {1}, ",
							   key, isUpdate);

			return newEtag;
		}


		public Guid AddDocumentInTransaction(TransactionInformation transactionInformation, string key, string data, Guid? etag, string metadata)
		{
			Api.JetSetCurrentIndex(session, Documents, "by_key");
			Api.MakeKey(session, Documents, key, Encoding.Unicode, MakeKeyGrbit.NewKey);
			var isUpdate = Api.TrySeek(session, Documents, SeekGrbit.SeekEQ);
			if (isUpdate)
			{
				EnsureNotLockedByTransaction(key, transactionInformation.Id);
				EnsureDocumentEtagMatchInTransaction(key, etag);
				using (var update = new Update(session, Documents, JET_prep.Replace))
				{
					Api.SetColumn(session, Documents, tableColumnsCache.DocumentsColumns["locked_by_transaction"], transactionInformation.Id.ToByteArray());
					update.Save();
				}
			}
			else
			{
				EnsureDocumentIsNotCreatedInAnotherTransaction(key, transactionInformation.Id);
			}
			EnsureTransactionExists(transactionInformation);
		    Guid newEtag = DocumentDatabase.CreateSequentialUuid();

			Api.JetSetCurrentIndex(session, DocumentsModifiedByTransactions, "by_key");
			Api.MakeKey(session, DocumentsModifiedByTransactions, key, Encoding.Unicode, MakeKeyGrbit.NewKey);
			var isUpdateInTransaction = Api.TrySeek(session, DocumentsModifiedByTransactions, SeekGrbit.SeekEQ);

			using (var update = new Update(session, DocumentsModifiedByTransactions, isUpdateInTransaction ? JET_prep.Replace : JET_prep.Insert))
			{
				Api.SetColumn(session, DocumentsModifiedByTransactions, tableColumnsCache.DocumentsModifiedByTransactionsColumns["key"], key, Encoding.Unicode);
				Api.SetColumn(session, DocumentsModifiedByTransactions, tableColumnsCache.DocumentsModifiedByTransactionsColumns["data"], Encoding.UTF8.GetBytes(data));
				Api.SetColumn(session, DocumentsModifiedByTransactions, tableColumnsCache.DocumentsModifiedByTransactionsColumns["etag"], newEtag.ToByteArray());
				Api.SetColumn(session, DocumentsModifiedByTransactions, tableColumnsCache.DocumentsModifiedByTransactionsColumns["metadata"], Encoding.UTF8.GetBytes(metadata));
				Api.SetColumn(session, DocumentsModifiedByTransactions, tableColumnsCache.DocumentsModifiedByTransactionsColumns["delete_document"], false);
				Api.SetColumn(session, DocumentsModifiedByTransactions, tableColumnsCache.DocumentsModifiedByTransactionsColumns["locked_by_transaction"], transactionInformation.Id.ToByteArray());

				update.Save();
			}
			logger.DebugFormat("Inserted a new document with key '{0}', doc length: {1}, update: {2}, in transaction: {3}",
							   key, data.Length, isUpdate, transactionInformation.Id);

			return newEtag;
		}


		public bool DeleteDocument(string key, Guid? etag)
		{
			Api.JetSetCurrentIndex(session, Documents, "by_key");
			Api.MakeKey(session, Documents, key, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, Documents, SeekGrbit.SeekEQ) == false)
			{
				logger.DebugFormat("Document with key '{0}' was not found, and considered deleted", key);
			    return false;
			}
			if (Api.TryMoveFirst(session, Details))
				Api.EscrowUpdate(session, Details, tableColumnsCache.DetailsColumns["document_count"], -1);
			
			EnsureDocumentEtagMatch(key, etag, "DELETE");
			EnsureNotLockedByTransaction(key, null);

			Api.JetDelete(session, Documents);
			logger.DebugFormat("Document with key '{0}' was deleted", key);
		    return true;
		}


		public void DeleteDocumentInTransaction(TransactionInformation transactionInformation, string key, Guid? etag)
		{
			Api.JetSetCurrentIndex(session, Documents, "by_key");
			Api.MakeKey(session, Documents, key, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, Documents, SeekGrbit.SeekEQ) == false)
			{
				logger.DebugFormat("Document with key '{0}' was not found, and considered deleted", key);
				return;
			}

			EnsureNotLockedByTransaction(key, transactionInformation.Id);
			EnsureDocumentEtagMatchInTransaction(key, etag);

			using (var update = new Update(session, Documents, JET_prep.Replace))
			{
				Api.SetColumn(session, Documents, tableColumnsCache.DocumentsColumns["locked_by_transaction"], transactionInformation.Id.ToByteArray());
				update.Save();
			}
			EnsureTransactionExists(transactionInformation);

		    Guid newEtag = DocumentDatabase.CreateSequentialUuid();

			Api.JetSetCurrentIndex(session, DocumentsModifiedByTransactions, "by_key");
			Api.MakeKey(session, DocumentsModifiedByTransactions, key, Encoding.Unicode, MakeKeyGrbit.NewKey);
			var isUpdateInTransaction = Api.TrySeek(session, DocumentsModifiedByTransactions, SeekGrbit.SeekEQ);

			using (var update = new Update(session, DocumentsModifiedByTransactions, isUpdateInTransaction ? JET_prep.Replace : JET_prep.Insert))
			{
				Api.SetColumn(session, DocumentsModifiedByTransactions, tableColumnsCache.DocumentsModifiedByTransactionsColumns["key"], key, Encoding.Unicode);
				Api.SetColumn(session, DocumentsModifiedByTransactions, tableColumnsCache.DocumentsModifiedByTransactionsColumns["data"],
					Api.RetrieveColumn(session, Documents, tableColumnsCache.DocumentsColumns["data"]));
				Api.SetColumn(session, DocumentsModifiedByTransactions, tableColumnsCache.DocumentsModifiedByTransactionsColumns["etag"], newEtag.ToByteArray());
				Api.SetColumn(session, DocumentsModifiedByTransactions, tableColumnsCache.DocumentsModifiedByTransactionsColumns["metadata"],
					Api.RetrieveColumnAsString(session, Documents, tableColumnsCache.DocumentsColumns["metadata"], Encoding.Unicode), Encoding.Unicode);
				Api.SetColumn(session, DocumentsModifiedByTransactions, tableColumnsCache.DocumentsModifiedByTransactionsColumns["delete_document"], true);
				Api.SetColumn(session, DocumentsModifiedByTransactions, tableColumnsCache.DocumentsModifiedByTransactionsColumns["locked_by_transaction"], transactionInformation.Id.ToByteArray());

				update.Save();
			}
		}


		public Tuple<int, int> FirstAndLastDocumentIds()
		{
			var item1 = 0;
			var item2 = 0;
			Api.JetSetCurrentIndex(session, Documents, "by_id");
			Api.MoveBeforeFirst(session, Documents);
			if (Api.TryMoveNext(session, Documents))
				item1 = Api.RetrieveColumnAsInt32(session, Documents, tableColumnsCache.DocumentsColumns["id"]).Value;
			Api.MoveAfterLast(session, Documents);
			if (Api.TryMovePrevious(session, Documents))
				item2 = Api.RetrieveColumnAsInt32(session, Documents, tableColumnsCache.DocumentsColumns["id"]).Value;
			return new Tuple<int, int>(item1, item2);
		}
	}
}
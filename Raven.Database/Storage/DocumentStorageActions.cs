using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using log4net;
using Microsoft.Isam.Esent.Interop;
using Newtonsoft.Json.Linq;
using Raven.Database.Data;
using Raven.Database.Exceptions;
using Raven.Database.Extensions;
using Raven.Database.Tasks;

namespace Raven.Database.Storage
{
	[CLSCompliant(false)]
	public class DocumentStorageActions : IDisposable
	{
		protected readonly JET_DBID dbid;
		public Table Documents { get; set; }
		protected readonly IDictionary<string, JET_COLUMNID> documentsColumns;
		public Table Transactions { get; set; }
		protected readonly IDictionary<string, JET_COLUMNID> transactionsColumns;

		public Table DocumentsModifiedByTransactions { get; set; }
		protected readonly IDictionary<string, JET_COLUMNID> documentsModifiedByTransactionsColumns;

		public Table Files { get; set; }
		protected readonly IDictionary<string, JET_COLUMNID> filesColumns;
		public Table IndexesStats { get; private set; }
		private readonly IDictionary<string, JET_COLUMNID> indexesStatsColumns;
		protected readonly ILog logger = LogManager.GetLogger(typeof (DocumentStorageActions));
		public Table MappedResults { get; private set; }
		private readonly IDictionary<string, JET_COLUMNID> mappedResultsColumns;
		protected readonly Session session;
		public Table Tasks { get; set; }
		protected readonly IDictionary<string, JET_COLUMNID> tasksColumns;
		private readonly Transaction transaction;
		private int innerTxCount;
		public Table Identity { get; set; }
		protected readonly IDictionary<string, JET_COLUMNID> identityColumns;
		
		[CLSCompliant(false)]
		[DebuggerHidden, DebuggerNonUserCode, DebuggerStepThrough]
		public DocumentStorageActions(
			JET_INSTANCE instance,
		    string database,
		    IDictionary<string, JET_COLUMNID> documentsColumns,
		    IDictionary<string, JET_COLUMNID> tasksColumns,
		    IDictionary<string, JET_COLUMNID> filesColumns,
		    IDictionary<string, JET_COLUMNID> indexesStatsColumns,
		    IDictionary<string, JET_COLUMNID> mappedResultsColumns, 
            IDictionary<string, JET_COLUMNID> documentsModifiedByTransactionsColumns, 
			IDictionary<string, JET_COLUMNID> transactionsColumns, 
			IDictionary<string, JET_COLUMNID> identityColumns)
		{
			try
			{
				session = new Session(instance);
				transaction = new Transaction(session);
				Api.JetOpenDatabase(session, database, null, out dbid, OpenDatabaseGrbit.None);

				Documents = new Table(session, dbid, "documents", OpenTableGrbit.None);
				Tasks = new Table(session, dbid, "tasks", OpenTableGrbit.None);
				Files = new Table(session, dbid, "files", OpenTableGrbit.None);
				IndexesStats = new Table(session, dbid, "indexes_stats", OpenTableGrbit.None);
				MappedResults = new Table(session, dbid, "mapped_results", OpenTableGrbit.None);
                DocumentsModifiedByTransactions = new Table(session, dbid, "documents_modified_by_transaction", OpenTableGrbit.None);
			    Transactions = new Table(session, dbid, "transactions", OpenTableGrbit.None);
				Identity = new Table(session, dbid, "identity_table", OpenTableGrbit.None);

				this.documentsColumns = documentsColumns;
				this.tasksColumns = tasksColumns;
				this.filesColumns = filesColumns;
				this.indexesStatsColumns = indexesStatsColumns;
				this.mappedResultsColumns = mappedResultsColumns;
                this.documentsModifiedByTransactionsColumns = documentsModifiedByTransactionsColumns;
                this.transactionsColumns = transactionsColumns;
				this.identityColumns = identityColumns;
			}
			catch (Exception)
			{
				Dispose();
				throw;
			}
		}

		public bool CommitCalled { get; set; }

		public IEnumerable<string> DocumentKeys
		{
			get
			{
				Api.MoveBeforeFirst(session, Documents);
				while (Api.TryMoveNext(session, Documents))
				{
					yield return
						Api.RetrieveColumnAsString(session, Documents, documentsColumns["key"], Encoding.Unicode);
				}
			}
		}

		#region IDisposable Members

		[DebuggerHidden, DebuggerNonUserCode, DebuggerStepThrough]
		public void Dispose()
		{
			if (Identity != null)
				Identity.Dispose();
            if(Transactions != null)
                Transactions.Dispose();

            if (DocumentsModifiedByTransactions!=null)
                DocumentsModifiedByTransactions.Dispose();

			if (MappedResults != null)
				MappedResults.Dispose();

			if (IndexesStats != null)
				IndexesStats.Dispose();

			if (Files != null)
				Files.Dispose();

			if (Documents != null)
				Documents.Dispose();

			if (Tasks != null)
				Tasks.Dispose();

			if (Equals(dbid, JET_DBID.Nil) == false && session != null)
				Api.JetCloseDatabase(session.JetSesid, dbid, CloseDatabaseGrbit.None);

			if (transaction != null)
				transaction.Dispose();

			if (session != null)
				session.Dispose();
		}

		#endregion

		public JsonDocument DocumentByKey(string key, TransactionInformation transactionInformation)
		{
		    byte[] data;
		    if (transactionInformation != null)
            {
                Api.JetSetCurrentIndex(session, DocumentsModifiedByTransactions, "by_key");
                Api.MakeKey(session, DocumentsModifiedByTransactions, key, Encoding.Unicode, MakeKeyGrbit.NewKey);
                if(Api.TrySeek(session, DocumentsModifiedByTransactions,SeekGrbit.SeekEQ))
                {
                    var txId = Api.RetrieveColumn(session, DocumentsModifiedByTransactions, documentsModifiedByTransactionsColumns["locked_by_transaction"]);
                    if(new Guid(txId) == transactionInformation.Id)
                    {
                        if (Api.RetrieveColumnAsBoolean(session, DocumentsModifiedByTransactions, documentsModifiedByTransactionsColumns["delete_document"]) == true)
                        {
                            logger.DebugFormat("Document with key '{0}' was deleted in transaction: {1}", key, transactionInformation.Id);
                            return null;
                        }
                        data = Api.RetrieveColumn(session, DocumentsModifiedByTransactions, documentsModifiedByTransactionsColumns["data"]);
                        logger.DebugFormat("Document with key '{0}' was found in transaction: {1}", key, transactionInformation.Id);
                        return new JsonDocument
                        {
                            Data = data,
                            Etag = new Guid(Api.RetrieveColumn(session, DocumentsModifiedByTransactions, documentsModifiedByTransactionsColumns["etag"])),
                            Key = Api.RetrieveColumnAsString(session, DocumentsModifiedByTransactions, documentsModifiedByTransactionsColumns["key"], Encoding.Unicode),
                            Metadata = JObject.Parse(Api.RetrieveColumnAsString(session, DocumentsModifiedByTransactions, documentsModifiedByTransactionsColumns["metadata"]))
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
			data = Api.RetrieveColumn(session, Documents, documentsColumns["data"]);
			logger.DebugFormat("Document with key '{0}' was found", key);
			return new JsonDocument
			{
				Data = data,
				Etag = new Guid(Api.RetrieveColumn(session, Documents, documentsColumns["etag"])),
				Key = Api.RetrieveColumnAsString(session, Documents, documentsColumns["key"], Encoding.Unicode),
				Metadata = JObject.Parse(Api.RetrieveColumnAsString(session, Documents, documentsColumns["metadata"]))
			};
		}

		public void Commit()
		{
			if (innerTxCount != 0)
				return;

			CommitCalled = true;
			transaction.Commit(CommitTransactionGrbit.LazyFlush);
		}

		public int GetNextIdentityValue(string name)
		{
			Api.JetSetCurrentIndex(session, Identity, "by_key");
			Api.MakeKey(session, Identity, name, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, Identity, SeekGrbit.SeekEQ) == false)
			{
				using(var update = new Update(session, Identity, JET_prep.Insert))
				{
					Api.SetColumn(session, Identity, identityColumns["key"],name, Encoding.Unicode);
					Api.SetColumn(session, Identity, identityColumns["val"], 1);

					update.Save();
				}
				return 1;
			}

			return Api.EscrowUpdate(session, Identity, identityColumns["val"], 1) + 1;
		}

		public Tuple<int, int> FirstAndLastDocumentIds()
		{
			var item1 = 0;
			var item2 = 0;
			Api.JetSetCurrentIndex(session, Documents, "by_id");
			Api.MoveBeforeFirst(session, Documents);
			if (Api.TryMoveNext(session, Documents))
				item1 = Api.RetrieveColumnAsInt32(session, Documents, documentsColumns["id"]).Value;
			Api.MoveAfterLast(session, Documents);
			if (Api.TryMovePrevious(session, Documents))
				item2 = Api.RetrieveColumnAsInt32(session, Documents, documentsColumns["id"]).Value;
			return new Tuple<int, int>(item1, item2);
		}

		public bool DoesTasksExistsForIndex(string name)
		{
			Api.JetSetCurrentIndex(session, Tasks, "by_index");
			Api.MakeKey(session, Tasks, name, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, Tasks, SeekGrbit.SeekEQ) == false)
			{
				Api.MakeKey(session, Tasks, "*", Encoding.Unicode, MakeKeyGrbit.NewKey);
				return Api.TrySeek(session, Tasks, SeekGrbit.SeekEQ);
			}
			return true;
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
				var id = Api.RetrieveColumnAsInt32(session, Documents, documentsColumns["id"],
				                                   RetrieveColumnGrbit.RetrieveFromIndex).Value;
				if (id > endId)
					break;

				var data = Api.RetrieveColumn(session, Documents, documentsColumns["data"]);
				logger.DebugFormat("Document with id '{0}' was found, doc length: {1}", id, data.Length);
				var json = Api.RetrieveColumnAsString(session, Documents, documentsColumns["metadata"],
				                                      Encoding.Unicode);
				var doc = new JsonDocument
				{
					Key = Api.RetrieveColumnAsString(session, Documents, documentsColumns["key"], Encoding.Unicode),
					Data = data,
					Etag = new Guid(Api.RetrieveColumn(session, Documents, documentsColumns["etag"])),
					Metadata = JObject.Parse(json)
				};
				yield return new Tuple<JsonDocument, int>(doc, id);
			} while (Api.TryMoveNext(session, Documents));
			hasMoreWork.Value = false;
		}

		public Guid AddDocument(string key, string data, Guid? etag, string metadata)
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
			}
		    Guid newEtag;
			DocumentDatabase.UuidCreateSequential(out newEtag);

			using (var update = new Update(session, Documents, isUpdate ? JET_prep.Replace : JET_prep.Insert))
			{
				Api.SetColumn(session, Documents, documentsColumns["key"], key, Encoding.Unicode);
				Api.SetColumn(session, Documents, documentsColumns["data"], Encoding.UTF8.GetBytes(data));
				Api.SetColumn(session, Documents, documentsColumns["etag"], newEtag.ToByteArray());
				Api.SetColumn(session, Documents, documentsColumns["metadata"], metadata, Encoding.Unicode);

				update.Save();
			}
			logger.DebugFormat("Inserted a new document with key '{0}', doc length: {1}, update: {2}, ",
			                   key, data.Length, isUpdate);

		    return newEtag;
		}

	    private void EnsureDocumentEtagMatch(string key, Guid? etag, string method)
	    {
	        var existingEtag = new Guid(Api.RetrieveColumn(session, Documents, documentsColumns["etag"]));
	        if (existingEtag != etag && etag != null)
	        {
	            throw new ConcurrencyException(method + " attempted on document '" + key +
	                                           "' using a non current etag")
	            {
	                ActualETag = etag.Value,
	                ExpectedETag = existingEtag
	            };
	        }
	    }

        private void EnsureDocumentEtagMatchInTransaction(string key, Guid? etag)
        {
            Api.JetSetCurrentIndex(session, DocumentsModifiedByTransactions, "by_key");
            Api.MakeKey(session, DocumentsModifiedByTransactions, key, Encoding.Unicode, MakeKeyGrbit.NewKey);
            Guid existingEtag;
            if (Api.TrySeek(session, DocumentsModifiedByTransactions, SeekGrbit.SeekEQ))
            {
                if (Api.RetrieveColumnAsBoolean(session, DocumentsModifiedByTransactions, documentsModifiedByTransactionsColumns["delete_document"]) == true)
                    return; // we ignore etags on deleted documents
                existingEtag = new Guid(Api.RetrieveColumn(session, DocumentsModifiedByTransactions, documentsModifiedByTransactionsColumns["etag"]));
            }
            else
            {
                existingEtag = new Guid(Api.RetrieveColumn(session, Documents, documentsColumns["etag"]));
            }
            if (existingEtag != etag && etag != null)
            {
                throw new ConcurrencyException("PUT attempted on document '" + key +
                                               "' using a non current etag")
                {
                    ActualETag = etag.Value,
                    ExpectedETag = existingEtag
                };
            }
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
                using(var update = new Update(session, Documents,JET_prep.Replace))
                {
                    Api.SetColumn(session, Documents, documentsColumns["locked_by_transaction"], transactionInformation.Id.ToByteArray()); 
                    update.Save();
                }
            }
            else
            {
                EnsureDocumentIsNotCreatedInAnotherTransaction(key, transactionInformation.Id);
            }
            EnsureTransactionExists(transactionInformation);
            Guid newEtag;
            DocumentDatabase.UuidCreateSequential(out newEtag);

            Api.JetSetCurrentIndex(session, DocumentsModifiedByTransactions, "by_key");
            Api.MakeKey(session, DocumentsModifiedByTransactions, key, Encoding.Unicode, MakeKeyGrbit.NewKey);
	        var isUpdateInTransaction = Api.TrySeek(session, DocumentsModifiedByTransactions, SeekGrbit.SeekEQ);

            using (var update = new Update(session, DocumentsModifiedByTransactions, isUpdateInTransaction ? JET_prep.Replace : JET_prep.Insert))
            {
                Api.SetColumn(session, DocumentsModifiedByTransactions, documentsModifiedByTransactionsColumns["key"], key, Encoding.Unicode);
                Api.SetColumn(session, DocumentsModifiedByTransactions, documentsModifiedByTransactionsColumns["data"], Encoding.UTF8.GetBytes(data));
                Api.SetColumn(session, DocumentsModifiedByTransactions, documentsModifiedByTransactionsColumns["etag"], newEtag.ToByteArray());
                Api.SetColumn(session, DocumentsModifiedByTransactions, documentsModifiedByTransactionsColumns["metadata"], metadata, Encoding.Unicode);
                Api.SetColumn(session, DocumentsModifiedByTransactions, documentsModifiedByTransactionsColumns["delete_document"], false);
                Api.SetColumn(session, DocumentsModifiedByTransactions, documentsModifiedByTransactionsColumns["locked_by_transaction"], transactionInformation.Id.ToByteArray());

                update.Save();
            }
	        logger.DebugFormat("Inserted a new document with key '{0}', doc length: {1}, update: {2}, in transaction: {3}",
	                           key, data.Length, isUpdate, transactionInformation.Id);

	        return newEtag;
        }

	    private void EnsureDocumentIsNotCreatedInAnotherTransaction(string key, Guid txId)
	    {
	        Api.JetSetCurrentIndex(session, DocumentsModifiedByTransactions,"by_key");
	        Api.MakeKey(session,DocumentsModifiedByTransactions, key, Encoding.Unicode, MakeKeyGrbit.NewKey);
            if (Api.TrySeek(session, DocumentsModifiedByTransactions, SeekGrbit.SeekEQ) == false)
                return;
	        byte[] docTxId = Api.RetrieveColumn(session, DocumentsModifiedByTransactions,documentsModifiedByTransactionsColumns["locked_by_transaction"]);
            if(new Guid(docTxId) != txId)
	        {
	            throw new ConcurrencyException("A document with key: '" + key+"' is currently created in another transaction");
	        }
	    }

	    private void EnsureTransactionExists(TransactionInformation transactionInformation)
	    {
            Api.JetSetCurrentIndex(session, Transactions, "by_tx_id");
            Api.MakeKey(session, Transactions, transactionInformation.Id.ToByteArray(), MakeKeyGrbit.NewKey);
	        var isUpdate = Api.TrySeek(session, Transactions, SeekGrbit.SeekEQ);
            using(var update = new Update(session, Transactions, isUpdate ? JET_prep.Replace : JET_prep.Insert))
            {
                Api.SetColumn(session, Transactions,transactionsColumns["tx_id"], transactionInformation.Id.ToByteArray());
                Api.SetColumn(session, Transactions, transactionsColumns["timeout"],
                              DateTime.UtcNow + transactionInformation.Timeout);
                update.Save();
            }
	    }

	    public void DeleteDocument(string key, Guid? etag)
		{
			Api.JetSetCurrentIndex(session, Documents, "by_key");
			Api.MakeKey(session, Documents, key, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, Documents, SeekGrbit.SeekEQ) == false)
			{
				logger.DebugFormat("Document with key '{0}' was not found, and considered deleted", key);
				return;
			}

			EnsureDocumentEtagMatch(key, etag,"DELETE");
		    EnsureNotLockedByTransaction(key, null);

		    Api.JetDelete(session, Documents);
			logger.DebugFormat("Document with key '{0}' was deleted", key);
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
                Api.SetColumn(session, Documents, documentsColumns["locked_by_transaction"], transactionInformation.Id.ToByteArray());
                update.Save();
            }
            EnsureTransactionExists(transactionInformation);

            Guid newEtag;
            DocumentDatabase.UuidCreateSequential(out newEtag);

            Api.JetSetCurrentIndex(session, DocumentsModifiedByTransactions, "by_key");
            Api.MakeKey(session, DocumentsModifiedByTransactions, key, Encoding.Unicode, MakeKeyGrbit.NewKey);
            var isUpdateInTransaction = Api.TrySeek(session, DocumentsModifiedByTransactions, SeekGrbit.SeekEQ);

            using (var update = new Update(session, DocumentsModifiedByTransactions, isUpdateInTransaction ? JET_prep.Replace : JET_prep.Insert))
            {
                Api.SetColumn(session, DocumentsModifiedByTransactions, documentsModifiedByTransactionsColumns["key"], key, Encoding.Unicode);
                Api.SetColumn(session, DocumentsModifiedByTransactions, documentsModifiedByTransactionsColumns["data"], 
                    Api.RetrieveColumn(session,Documents, documentsColumns["data"]));
                Api.SetColumn(session, DocumentsModifiedByTransactions, documentsModifiedByTransactionsColumns["etag"], newEtag.ToByteArray());
                Api.SetColumn(session, DocumentsModifiedByTransactions, documentsModifiedByTransactionsColumns["metadata"],
                    Api.RetrieveColumnAsString(session, Documents, documentsColumns["metadata"],Encoding.Unicode), Encoding.Unicode);
                Api.SetColumn(session, DocumentsModifiedByTransactions, documentsModifiedByTransactionsColumns["delete_document"], true);
                Api.SetColumn(session, DocumentsModifiedByTransactions, documentsModifiedByTransactionsColumns["locked_by_transaction"], transactionInformation.Id.ToByteArray());

                update.Save();
            }
        }

	    private void EnsureNotLockedByTransaction(string key, Guid? currentTransaction)
	    {
	        byte[] txId = Api.RetrieveColumn(session, Documents, documentsColumns["locked_by_transaction"]);
	        if (txId == null)
	        {
	            return;
	        }
	        var guid = new Guid(txId);
            if (currentTransaction != null && guid == currentTransaction.Value)
                return;

	        Api.JetSetCurrentIndex(session, Transactions, "by_tx_id");
            Api.MakeKey(session, Transactions, txId,MakeKeyGrbit.NewKey);
            if(Api.TrySeek(session, Transactions, SeekGrbit.SeekEQ)==false)
            {
                //This is a bug, probably... because it means that we have a missing
                // transaction, we are going to reset it
                ResetTransactionOnCurrentDocument();
                return;
            }
	        var timeout = Api.RetrieveColumnAsDateTime(session, Transactions, transactionsColumns["timeout"]);
            if(DateTime.UtcNow > timeout)// the timeout for the transaction has passed
            {
                RollbackTransaction(guid);
                return;
            }
	        throw new ConcurrencyException("Document '" + key + "' is locked by transacton: " + guid);
	    }

	    public void RollbackTransaction(Guid txId)
	    {
	        CompleteTransaction(txId, doc =>
	        {
	            Api.MakeKey(session, Documents, doc.Key, Encoding.Unicode, MakeKeyGrbit.NewKey);
	            if (Api.TrySeek(session, Documents, SeekGrbit.SeekEQ))
	            {
	                ResetTransactionOnCurrentDocument();
	            }
	        });
	    }

	    public void CompleteTransaction(Guid txId, Action<DocumentInTransactionData> perDocumentModified)
	    {
	        Api.JetSetCurrentIndex(session, Transactions, "by_tx_id");
	        Api.MakeKey(session, Transactions, txId, MakeKeyGrbit.NewKey);
	        if (Api.TrySeek(session, Transactions, SeekGrbit.SeekEQ))
	            Api.JetDelete(session, Transactions);
            
	        Api.JetSetCurrentIndex(session, DocumentsModifiedByTransactions, "by_tx");
	        Api.MakeKey(session, DocumentsModifiedByTransactions, txId.ToByteArray(), MakeKeyGrbit.NewKey);
	        if (Api.TrySeek(session, DocumentsModifiedByTransactions, SeekGrbit.SeekEQ) == false)
	            return;
	        Api.MakeKey(session, DocumentsModifiedByTransactions, txId.ToByteArray(), MakeKeyGrbit.NewKey);
	        Api.JetSetIndexRange(session, DocumentsModifiedByTransactions,
	                             SetIndexRangeGrbit.RangeInclusive | SetIndexRangeGrbit.RangeUpperLimit);

	        do
	        {
	            var documentInTransactionData = new DocumentInTransactionData
	            {
	                Data = Encoding.UTF8.GetString(Api.RetrieveColumn(session, DocumentsModifiedByTransactions, documentsModifiedByTransactionsColumns["data"])),
	                Delete = Api.RetrieveColumnAsBoolean(session,DocumentsModifiedByTransactions,documentsModifiedByTransactionsColumns["delete_document"]).Value,
	                Etag = new Guid(Api.RetrieveColumn(session, DocumentsModifiedByTransactions, documentsModifiedByTransactionsColumns["etag"])),
	                Key = Api.RetrieveColumnAsString(session, DocumentsModifiedByTransactions, documentsModifiedByTransactionsColumns["key"],Encoding.Unicode),
	                Metadata = Api.RetrieveColumnAsString(session, DocumentsModifiedByTransactions, documentsModifiedByTransactionsColumns["metadata"], Encoding.Unicode),
	            };
                Api.JetDelete(session, DocumentsModifiedByTransactions);
                perDocumentModified(documentInTransactionData);
	        } while (Api.TryMoveNext(session, DocumentsModifiedByTransactions));
	    }

	    private void ResetTransactionOnCurrentDocument()
	    {
	        using(var update = new Update(session, Documents, JET_prep.Replace))
	        {
	            Api.SetColumn(session, Documents, documentsColumns["locked_by_transaction"], null);
	            update.Save();
	        }
	    }

	    public void AddTask(Task task)
		{
			using (var update = new Update(session, Tasks, JET_prep.Insert))
			{
				Api.SetColumn(session, Tasks, tasksColumns["task"], task.AsString(), Encoding.Unicode);
				Api.SetColumn(session, Tasks, tasksColumns["for_index"], task.Index, Encoding.Unicode);

				update.Save();
			}
			if (logger.IsDebugEnabled)
				logger.DebugFormat("New task '{0}'", task.AsString());
		}

		public void AddAttachment(string key, Guid? etag, byte[] data, string headers)
		{
			Api.JetSetCurrentIndex(session, Files, "by_name");
			Api.MakeKey(session, Files, key, Encoding.Unicode, MakeKeyGrbit.NewKey);
			var isUpdate = Api.TrySeek(session, Files, SeekGrbit.SeekEQ);
			if (isUpdate)
			{
				var existingEtag = new Guid(Api.RetrieveColumn(session, Files, filesColumns["etag"]));
				if (existingEtag != etag && etag != null)
				{
					throw new ConcurrencyException("PUT attempted on attachment '" + key +
						"' using a non current etag")
					{
						ActualETag = etag.Value,
						ExpectedETag = existingEtag
					};
				}
			}

			Guid newETag;
			DocumentDatabase.UuidCreateSequential(out newETag);
			using (var update = new Update(session, Files, isUpdate ? JET_prep.Replace : JET_prep.Insert))
			{
				Api.SetColumn(session, Files, filesColumns["name"], key, Encoding.Unicode);
				Api.SetColumn(session, Files, filesColumns["data"], data);
				Api.SetColumn(session, Files, filesColumns["etag"], newETag.ToByteArray());
				Api.SetColumn(session, Files, filesColumns["metadata"], headers, Encoding.Unicode);

				update.Save();
			}
			logger.DebugFormat("Adding attachment {0}", key);
		}

		public void DeleteAttachment(string key, Guid? etag)
		{
			Api.JetSetCurrentIndex(session, Files, "by_name");
			Api.MakeKey(session, Files, key, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, Files, SeekGrbit.SeekEQ) == false)
			{
				logger.DebugFormat("Attachment with key '{0}' was not found, and considered deleted", key);
				return;
			}
			var fileEtag = new Guid(Api.RetrieveColumn(session, Files, filesColumns["etag"]));
			if (fileEtag != etag && etag != null)
			{
				throw new ConcurrencyException("DELETE attempted on attachment '" + key +
					"' using a non current etag")
				{
					ActualETag = etag.Value,
					ExpectedETag = fileEtag
				};
			}

			Api.JetDelete(session, Files);
			logger.DebugFormat("Attachment with key '{0}' was deleted", key);
		}

		public Attachment GetAttachment(string key)
		{
			Api.JetSetCurrentIndex(session, Files, "by_name");
			Api.MakeKey(session, Files, key, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, Files, SeekGrbit.SeekEQ) == false)
			{
				return null;
			}

			var metadata = Api.RetrieveColumnAsString(session, Files, filesColumns["metadata"], Encoding.Unicode);
			return new Attachment
			{
				Data = Api.RetrieveColumn(session, Files, filesColumns["data"]),
				Etag = new Guid(Api.RetrieveColumn(session, Files, filesColumns["etag"])),
				Metadata = JObject.Parse(metadata)
			};
		}

		public string GetFirstTask()
		{
			Api.MoveBeforeFirst(session, Tasks);
			while (Api.TryMoveNext(session, Tasks))
			{
				try
				{
					Api.JetGetLock(session, Tasks, GetLockGrbit.Write);
				}
				catch (EsentErrorException e)
				{
					if (e.Error != JET_err.WriteConflict)
						throw;
				}
				return Api.RetrieveColumnAsString(session, Tasks, tasksColumns["task"], Encoding.Unicode);
			}
			return null;
		}

		public void CompleteCurrentTask()
		{
			Api.JetDelete(session, Tasks);
		}

		public void PushTx()
		{
			innerTxCount++;
		}

		public void PopTx()
		{
			innerTxCount--;
		}

		public int GetDocumentsCount()
		{
			int val;
			Api.JetIndexRecordCount(session, Documents, out val, 0);
			return val;
		}

		public void SetCurrentIndexStatsTo(string index)
		{
			Api.JetSetCurrentIndex(session, IndexesStats, "by_key");
			Api.MakeKey(session, IndexesStats, index, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, IndexesStats, SeekGrbit.SeekEQ) == false)
				throw new InvalidOperationException("There is no index named: " + index);
		}

		public void IncrementIndexingAttempt()
		{
			Api.EscrowUpdate(session, IndexesStats, indexesStatsColumns["attempts"], 1);
		}

		public void IncrementSuccessIndexing()
		{
			Api.EscrowUpdate(session, IndexesStats, indexesStatsColumns["successes"], 1);
		}

		public void IncrementIndexingFailure()
		{
			Api.EscrowUpdate(session, IndexesStats, indexesStatsColumns["errors"], 1);
		}

		public IEnumerable<IndexStats> GetIndexesStats()
		{
			Api.MoveBeforeFirst(session, IndexesStats);
			while (Api.TryMoveNext(session, IndexesStats))
			{
				yield return new IndexStats
				{
					Name = Api.RetrieveColumnAsString(session, IndexesStats, indexesStatsColumns["key"]),
					IndexingAttempts =
						Api.RetrieveColumnAsInt32(session, IndexesStats, indexesStatsColumns["attempts"]).Value,
					IndexingSuccesses =
						Api.RetrieveColumnAsInt32(session, IndexesStats, indexesStatsColumns["successes"]).Value,
					IndexingErrors =
						Api.RetrieveColumnAsInt32(session, IndexesStats, indexesStatsColumns["errors"]).Value,
				};
			}
		}

		public void AddIndex(string name)
		{
			using (var update = new Update(session, IndexesStats, JET_prep.Insert))
			{
				Api.SetColumn(session, IndexesStats, indexesStatsColumns["key"], name, Encoding.Unicode);

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
			Api.EscrowUpdate(session, IndexesStats, indexesStatsColumns["attempts"], -1);
		}

		public IndexFailureInformation GetFailureRate(string index)
		{
			SetCurrentIndexStatsTo(index);
			return new IndexFailureInformation
			{
				Name = index,
				Attempts = Api.RetrieveColumnAsInt32(session, IndexesStats, indexesStatsColumns["attempts"]).Value,
				Errors = Api.RetrieveColumnAsInt32(session, IndexesStats, indexesStatsColumns["errors"]).Value,
				Successes = Api.RetrieveColumnAsInt32(session, IndexesStats, indexesStatsColumns["successes"]).Value
			};
		}

		public void PutMappedResult(string view, string docId, string reduceKey, string data)
		{
			Api.JetSetCurrentIndex(session, MappedResults, "by_pk");
			Api.MakeKey(session, MappedResults, view, Encoding.Unicode, MakeKeyGrbit.NewKey);
			Api.MakeKey(session, MappedResults, docId, Encoding.Unicode, MakeKeyGrbit.None);
			Api.MakeKey(session, MappedResults, reduceKey, Encoding.Unicode, MakeKeyGrbit.None);
			var isUpdate = Api.TrySeek(session, MappedResults, SeekGrbit.SeekEQ);

			using (var update = new Update(session, MappedResults, isUpdate ? JET_prep.Replace : JET_prep.Insert))
			{
				Api.SetColumn(session, MappedResults, mappedResultsColumns["view"], view, Encoding.Unicode);
				Api.SetColumn(session, MappedResults, mappedResultsColumns["document_key"], docId, Encoding.Unicode);
				Api.SetColumn(session, MappedResults, mappedResultsColumns["reduce_key"], reduceKey, Encoding.Unicode);
				Api.SetColumn(session, MappedResults, mappedResultsColumns["data"], Encoding.UTF8.GetBytes(data));

				update.Save();
			}
		}

		public IEnumerable<string> GetMappedResults(string view, string reduceKey)
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
				var bytes = Api.RetrieveColumn(session, MappedResults, mappedResultsColumns["data"]);
				yield return Encoding.UTF8.GetString(bytes);
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
				var reduceKey = Api.RetrieveColumnAsString(session, MappedResults, mappedResultsColumns["reduce_key"],
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
				Api.JetDelete(session, MappedResults);
			} while (Api.TryMoveNext(session, MappedResults));
		}

	}
}
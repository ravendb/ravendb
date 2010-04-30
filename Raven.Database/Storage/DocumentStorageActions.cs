using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Diagnostics;
using System.Text;
using log4net;
using Microsoft.Isam.Esent.Interop;
using Newtonsoft.Json.Linq;
using Raven.Database.Data;
using Raven.Database.Exceptions;
using Raven.Database.Extensions;
using Raven.Database.Plugins;
using Raven.Database.Tasks;
using Raven.Database.Json;

namespace Raven.Database.Storage
{
	[CLSCompliant(false)]
	public class DocumentStorageActions : IDisposable
	{
		public event Action OnCommit = delegate { }; 
		private readonly TableColumnsCache tableColumnsCache;
		protected readonly JET_DBID dbid;
		private Table documents;
		protected Table Documents
		{
			get { return documents ?? (documents = new Table(session, dbid, "documents", OpenTableGrbit.None)); }
		}

		private Table transactions;
		protected Table Transactions
		{
			get { return transactions ?? (transactions = new Table(session, dbid, "transactions", OpenTableGrbit.None)); }
		}

		private Table documentsModifiedByTransactions;
		protected Table DocumentsModifiedByTransactions
		{
			get
			{
				return documentsModifiedByTransactions ??
					(documentsModifiedByTransactions =
						new Table(session, dbid, "documents_modified_by_transaction", OpenTableGrbit.None));
			}
		}


		private Table files;
		protected Table Files
		{
			get { return files ?? (files = new Table(session, dbid, "files", OpenTableGrbit.None)); }
		}

		private Table indexesStats;
		protected Table IndexesStats
		{
			get { return indexesStats ?? (indexesStats = new Table(session, dbid, "indexes_stats", OpenTableGrbit.None)); }
		}

		protected readonly ILog logger = LogManager.GetLogger(typeof(DocumentStorageActions));
		private Table mappedResults;
		protected Table MappedResults
		{
			get { return mappedResults ?? (mappedResults = new Table(session, dbid, "mapped_results", OpenTableGrbit.None)); }
		}

		protected readonly Session session;
		private Table tasks;

		protected Table Tasks
		{
			get { return tasks ?? (tasks = new Table(session, dbid, "tasks", OpenTableGrbit.None)); }
		}

		private readonly Transaction transaction;
		private Table identity;
		protected Table Identity
		{
			get { return identity ?? (identity = new Table(session, dbid, "identity_table", OpenTableGrbit.None)); }
		}

		private Table details;
		protected Table Details
		{
			get { return details ?? (details = new Table(session, dbid, "details", OpenTableGrbit.None)); }
		}

		[CLSCompliant(false)]
		[DebuggerHidden, DebuggerNonUserCode, DebuggerStepThrough]
		public DocumentStorageActions(JET_INSTANCE instance, string database, TableColumnsCache tableColumnsCache)
		{
			this.tableColumnsCache = tableColumnsCache;
			try
			{
				session = new Session(instance);
				transaction = new Transaction(session);
				Api.JetOpenDatabase(session, database, null, out dbid, OpenDatabaseGrbit.None);
			}
			catch (Exception)
			{
				Dispose();
				throw;
			}
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

		#region IDisposable Members

		[DebuggerHidden, DebuggerNonUserCode, DebuggerStepThrough]
		public void Dispose()
		{
			if (details != null)
				details.Dispose();

			if (identity != null)
				identity.Dispose();

			if (transactions != null)
				transactions.Dispose();

			if (documentsModifiedByTransactions != null)
				documentsModifiedByTransactions.Dispose();

			if (mappedResults != null)
				mappedResults.Dispose();

			if (indexesStats != null)
				indexesStats.Dispose();

			if (files != null)
				files.Dispose();

			if (documents != null)
				documents.Dispose();

			if (tasks != null)
				tasks.Dispose();

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
							Data = data,
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
				Data = data,
				Etag = existingEtag,
				Key = Api.RetrieveColumnAsString(session, Documents, tableColumnsCache.DocumentsColumns["key"], Encoding.Unicode),
				Metadata = Api.RetrieveColumn(session, Documents, tableColumnsCache.DocumentsColumns["metadata"]).ToJObject()
			};
		}

		public void Commit()
		{
			transaction.Commit(CommitTransactionGrbit.LazyFlush);
			OnCommit();
		}

		public int GetNextIdentityValue(string name)
		{
			Api.JetSetCurrentIndex(session, Identity, "by_key");
			Api.MakeKey(session, Identity, name, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, Identity, SeekGrbit.SeekEQ) == false)
			{
				using (var update = new Update(session, Identity, JET_prep.Insert))
				{
					Api.SetColumn(session, Identity, tableColumnsCache.IdentityColumns["key"], name, Encoding.Unicode);
					Api.SetColumn(session, Identity, tableColumnsCache.IdentityColumns["val"], 1);

					update.Save();
				}
				return 1;
			}

			return Api.EscrowUpdate(session, Identity, tableColumnsCache.IdentityColumns["val"], 1) + 1;
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
					Data = data,
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
			Guid newEtag;
			DocumentDatabase.UuidCreateSequential(out newEtag);

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

		private void EnsureDocumentEtagMatch(string key, Guid? etag, string method)
		{
			var existingEtag = new Guid(Api.RetrieveColumn(session, Documents, tableColumnsCache.DocumentsColumns["etag"]));
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
				if (Api.RetrieveColumnAsBoolean(session, DocumentsModifiedByTransactions, tableColumnsCache.DocumentsModifiedByTransactionsColumns["delete_document"]) == true)
					return; // we ignore etags on deleted documents
				existingEtag = new Guid(Api.RetrieveColumn(session, DocumentsModifiedByTransactions, tableColumnsCache.DocumentsModifiedByTransactionsColumns["etag"]));
			}
			else
			{
				existingEtag = new Guid(Api.RetrieveColumn(session, Documents, tableColumnsCache.DocumentsColumns["etag"]));
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
			Guid newEtag;
			DocumentDatabase.UuidCreateSequential(out newEtag);

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

		private void EnsureDocumentIsNotCreatedInAnotherTransaction(string key, Guid txId)
		{
			Api.JetSetCurrentIndex(session, DocumentsModifiedByTransactions, "by_key");
			Api.MakeKey(session, DocumentsModifiedByTransactions, key, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, DocumentsModifiedByTransactions, SeekGrbit.SeekEQ) == false)
				return;
			byte[] docTxId = Api.RetrieveColumn(session, DocumentsModifiedByTransactions, tableColumnsCache.DocumentsModifiedByTransactionsColumns["locked_by_transaction"]);
			if (new Guid(docTxId) != txId)
			{
				throw new ConcurrencyException("A document with key: '" + key + "' is currently created in another transaction");
			}
		}

		private void EnsureTransactionExists(TransactionInformation transactionInformation)
		{
			Api.JetSetCurrentIndex(session, Transactions, "by_tx_id");
			Api.MakeKey(session, Transactions, transactionInformation.Id.ToByteArray(), MakeKeyGrbit.NewKey);
			var isUpdate = Api.TrySeek(session, Transactions, SeekGrbit.SeekEQ);
			using (var update = new Update(session, Transactions, isUpdate ? JET_prep.Replace : JET_prep.Insert))
			{
				Api.SetColumn(session, Transactions, tableColumnsCache.TransactionsColumns["tx_id"], transactionInformation.Id.ToByteArray());
				Api.SetColumn(session, Transactions, tableColumnsCache.TransactionsColumns["timeout"],
							  DateTime.UtcNow + transactionInformation.Timeout);
				update.Save();
			}
		}

		public void DeleteDocument(string key, Guid? etag)
		{
			if (Api.TryMoveFirst(session, Details))
				Api.EscrowUpdate(session, Details, tableColumnsCache.DetailsColumns["document_count"], -1);
			Api.JetSetCurrentIndex(session, Documents, "by_key");
			Api.MakeKey(session, Documents, key, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, Documents, SeekGrbit.SeekEQ) == false)
			{
				logger.DebugFormat("Document with key '{0}' was not found, and considered deleted", key);
				return;
			}

			EnsureDocumentEtagMatch(key, etag, "DELETE");
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
				Api.SetColumn(session, Documents, tableColumnsCache.DocumentsColumns["locked_by_transaction"], transactionInformation.Id.ToByteArray());
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

		private void EnsureNotLockedByTransaction(string key, Guid? currentTransaction)
		{
			byte[] txId = Api.RetrieveColumn(session, Documents, tableColumnsCache.DocumentsColumns["locked_by_transaction"]);
			if (txId == null)
			{
				return;
			}
			var guid = new Guid(txId);
			if (currentTransaction != null && guid == currentTransaction.Value)
				return;

			Api.JetSetCurrentIndex(session, Transactions, "by_tx_id");
			Api.MakeKey(session, Transactions, txId, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, Transactions, SeekGrbit.SeekEQ) == false)
			{
				//This is a bug, probably... because it means that we have a missing
				// transaction, we are going to reset it
				ResetTransactionOnCurrentDocument();
				return;
			}
			var timeout = Api.RetrieveColumnAsDateTime(session, Transactions, tableColumnsCache.TransactionsColumns["timeout"]);
			if (DateTime.UtcNow > timeout)// the timeout for the transaction has passed
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
			Api.JetSetIndexRange(session, DocumentsModifiedByTransactions, SetIndexRangeGrbit.RangeInclusive | SetIndexRangeGrbit.RangeUpperLimit);

			do
			{
				var documentInTransactionData = new DocumentInTransactionData
				{
					Data = Api.RetrieveColumn(session, DocumentsModifiedByTransactions, tableColumnsCache.DocumentsModifiedByTransactionsColumns["data"]),
					Delete = Api.RetrieveColumnAsBoolean(session, DocumentsModifiedByTransactions, tableColumnsCache.DocumentsModifiedByTransactionsColumns["delete_document"]).Value,
					Etag = new Guid(Api.RetrieveColumn(session, DocumentsModifiedByTransactions, tableColumnsCache.DocumentsModifiedByTransactionsColumns["etag"])),
					Key = Api.RetrieveColumnAsString(session, DocumentsModifiedByTransactions, tableColumnsCache.DocumentsModifiedByTransactionsColumns["key"], Encoding.Unicode),
					Metadata = Api.RetrieveColumn(session, DocumentsModifiedByTransactions, tableColumnsCache.DocumentsModifiedByTransactionsColumns["metadata"]),
				};
				Api.JetDelete(session, DocumentsModifiedByTransactions);
				perDocumentModified(documentInTransactionData);
			} while (Api.TryMoveNext(session, DocumentsModifiedByTransactions));
		}

		private void ResetTransactionOnCurrentDocument()
		{
			using (var update = new Update(session, Documents, JET_prep.Replace))
			{
				Api.SetColumn(session, Documents, tableColumnsCache.DocumentsColumns["locked_by_transaction"], null);
				update.Save();
			}
		}

		public void AddTask(Task task)
		{
			int actualBookmarkSize;
			var bookmark = new byte[SystemParameters.BookmarkMost];
			using (var update = new Update(session, Tasks, JET_prep.Insert))
			{
				Api.SetColumn(session, Tasks, tableColumnsCache.TasksColumns["task"], task.AsString(), Encoding.Unicode);
				Api.SetColumn(session, Tasks, tableColumnsCache.TasksColumns["for_index"], task.Index, Encoding.Unicode);
				Api.SetColumn(session, Tasks, tableColumnsCache.TasksColumns["task_type"], task.Type, Encoding.Unicode);
				Api.SetColumn(session, Tasks, tableColumnsCache.TasksColumns["supports_merging"], task.SupportsMerging);

				update.Save(bookmark, bookmark.Length, out actualBookmarkSize);
			}
			Api.JetGotoBookmark(session, Tasks, bookmark, actualBookmarkSize);
			var taskId = Api.RetrieveColumnAsInt32(session, Tasks, tableColumnsCache.TasksColumns["id"]);
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
				var existingEtag = new Guid(Api.RetrieveColumn(session, Files, tableColumnsCache.FilesColumns["etag"]));
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
			else
			{
				if (Api.TryMoveFirst(session, Details))
					Api.EscrowUpdate(session, Details, tableColumnsCache.DetailsColumns["attachment_count"], 1);
			}

			Guid newETag;
			DocumentDatabase.UuidCreateSequential(out newETag);
			using (var update = new Update(session, Files, isUpdate ? JET_prep.Replace : JET_prep.Insert))
			{
				Api.SetColumn(session, Files, tableColumnsCache.FilesColumns["name"], key, Encoding.Unicode);
				Api.SetColumn(session, Files, tableColumnsCache.FilesColumns["data"], data);
				Api.SetColumn(session, Files, tableColumnsCache.FilesColumns["etag"], newETag.ToByteArray());
				Api.SetColumn(session, Files, tableColumnsCache.FilesColumns["metadata"], headers, Encoding.Unicode);

				update.Save();
			}
			logger.DebugFormat("Adding attachment {0}", key);
		}

		public void DeleteAttachment(string key, Guid? etag)
		{
			if (Api.TryMoveFirst(session, Details))
				Api.EscrowUpdate(session, Details, tableColumnsCache.DetailsColumns["attachment_count"], -1);
			Api.JetSetCurrentIndex(session, Files, "by_name");
			Api.MakeKey(session, Files, key, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, Files, SeekGrbit.SeekEQ) == false)
			{
				logger.DebugFormat("Attachment with key '{0}' was not found, and considered deleted", key);
				return;
			}
			var fileEtag = new Guid(Api.RetrieveColumn(session, Files, tableColumnsCache.FilesColumns["etag"]));
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

			var metadata = Api.RetrieveColumnAsString(session, Files, tableColumnsCache.FilesColumns["metadata"], Encoding.Unicode);
			return new Attachment
			{
				Data = Api.RetrieveColumn(session, Files, tableColumnsCache.FilesColumns["data"]),
				Etag = new Guid(Api.RetrieveColumn(session, Files, tableColumnsCache.FilesColumns["etag"])),
				Metadata = JObject.Parse(metadata)
			};
		}

		public bool HasTasks
		{
			get
			{
				return Api.TryMoveFirst(session, Tasks);
			}
		}

		public int ApproximateTaskCount
		{
			get
			{
				if (Api.TryMoveFirst(session, Tasks) == false)
					return 0;
				var first = (int)Api.RetrieveColumnAsInt32(session, Tasks, tableColumnsCache.TasksColumns["id"]);
				if (Api.TryMoveLast(session, Tasks) == false)
					return 0;
				var last = (int)Api.RetrieveColumnAsInt32(session, Tasks, tableColumnsCache.TasksColumns["id"]);
				return last - first;
			}
		}

		public Task GetMergedTask()
		{
			Api.MoveBeforeFirst(session, Tasks);
			while (Api.TryMoveNext(session, Tasks))
			{
				var taskAsString = Api.RetrieveColumnAsString(session, Tasks, tableColumnsCache.TasksColumns["task"], Encoding.Unicode);
				try
				{
					Api.JetDelete(session, Tasks);
				}
				catch (EsentErrorException e)
				{
					if (e.Error != JET_err.WriteConflict)
						throw;
				}
				Task task;
				try
				{
					task = Task.ToTask(taskAsString);
				}
				catch (Exception e)
				{
					logger.ErrorFormat(e, "Could not create instance of a task: {0}", taskAsString);
					continue;
				}

				MergeSimilarTasks(task);
				return task;
			}
			return null;
		}

		private void MergeSimilarTasks(Task task)
		{
			if (task.SupportsMerging == false)
				return;

			Api.JetSetCurrentIndex(session, Tasks, "mergables_by_task_type");
			Api.MakeKey(session, Tasks, true, MakeKeyGrbit.NewKey);
			Api.MakeKey(session, Tasks, task.Index, Encoding.Unicode, MakeKeyGrbit.None);
			Api.MakeKey(session, Tasks, task.Type, Encoding.Unicode, MakeKeyGrbit.None);
			// there are no tasks matching the current one, just return
			if (Api.TrySeek(session, Tasks, SeekGrbit.SeekEQ) == false)
			{
				return;
			}

			Api.MakeKey(session, Tasks, true, MakeKeyGrbit.NewKey);
			Api.MakeKey(session, Tasks, task.Index, Encoding.Unicode, MakeKeyGrbit.None);
			Api.MakeKey(session, Tasks, task.Type, Encoding.Unicode, MakeKeyGrbit.None);
			Api.JetSetIndexRange(session, Tasks, SetIndexRangeGrbit.RangeInclusive | SetIndexRangeGrbit.RangeUpperLimit);
			do
			{
				try
				{
					var taskAsString = Api.RetrieveColumnAsString(session, Tasks, tableColumnsCache.TasksColumns["task"], Encoding.Unicode);
					Task existingTask;
					try
					{
						existingTask = Task.ToTask(taskAsString);
					}
					catch (Exception e)
					{
						logger.ErrorFormat(e, "Could not create instance of a task: {0}", taskAsString);
						Api.JetDelete(session, Tasks);
						continue;
					}
					if (task.TryMerge(existingTask) == false)
						continue;
					Api.JetDelete(session, Tasks);
				}
				catch (EsentErrorException e)
				{
					if (e.Error == JET_err.WriteConflict)
						continue;
					throw;
				}
			} while (
					task.SupportsMerging &&
					Api.TryMoveNext(session, Tasks)
				);
		}

		public int GetDocumentsCount()
		{
			if (Api.TryMoveFirst(session, Details))
				return Api.RetrieveColumnAsInt32(session, Details, tableColumnsCache.DetailsColumns["document_count"]).Value;
			return 0;
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
				};
			}
		}

		public void AddIndex(string name)
		{
			using (var update = new Update(session, IndexesStats, JET_prep.Insert))
			{
				Api.SetColumn(session, IndexesStats, tableColumnsCache.IndexesStatsColumns["key"], name, Encoding.Unicode);

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
				Api.JetDelete(session, MappedResults);
			} while (Api.TryMoveNext(session, MappedResults));
		}
	}
}
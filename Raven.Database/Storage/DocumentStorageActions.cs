using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using log4net;
using Microsoft.Isam.Esent.Interop;
using Newtonsoft.Json.Linq;
using Raven.Database.Cache;
using Raven.Database.Data;
using Raven.Database.Exceptions;
using Raven.Database.Extensions;
using Raven.Database.Json;
using Raven.Database.Tasks;

namespace Raven.Database.Storage
{
	[CLSCompliant(false)]
	public class DocumentStorageActions : IDisposable
	{
		private readonly IDictionary<string, JET_COLUMNID> detailsColumns;
		protected readonly JET_DBID dbid;
		private Table documents;
		protected Table Documents
		{
			get { return documents ?? (documents = new Table(session, dbid, "documents", OpenTableGrbit.None)); }
		}

		protected readonly IDictionary<string, JET_COLUMNID> documentsColumns;
		private Table transactions;
		protected Table Transactions
		{
			get { return transactions ?? (transactions = new Table(session, dbid, "transactions", OpenTableGrbit.None)); }
		}

		protected readonly IDictionary<string, JET_COLUMNID> transactionsColumns;

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

		protected readonly IDictionary<string, JET_COLUMNID> documentsModifiedByTransactionsColumns;

		private Table files;
		protected Table Files
		{
			get { return files ?? (files = new Table(session, dbid, "files", OpenTableGrbit.None)); }
		}

		protected readonly IDictionary<string, JET_COLUMNID> filesColumns;
		private Table indexesStats;
		protected Table IndexesStats
		{
			get { return indexesStats ?? (indexesStats = new Table(session, dbid, "indexes_stats", OpenTableGrbit.None)); }
		}

		private readonly IDictionary<string, JET_COLUMNID> indexesStatsColumns;
		protected readonly ILog logger = LogManager.GetLogger(typeof(DocumentStorageActions));
		private Table mappedResults;
		protected Table MappedResults
		{
			get { return mappedResults ?? (mappedResults = new Table(session, dbid, "mapped_results", OpenTableGrbit.None)); }
		}

		private readonly IDictionary<string, JET_COLUMNID> mappedResultsColumns;
		protected readonly Session session;
		private Table tasks;

		protected Table Tasks
		{
			get { return tasks ?? (tasks = new Table(session, dbid, "tasks", OpenTableGrbit.None)); }
		}

		protected readonly IDictionary<string, JET_COLUMNID> tasksColumns;
		private readonly Transaction transaction;
		private Table identity;
		protected Table Identity
		{
			get { return identity ?? (identity = new Table(session, dbid, "identity_table", OpenTableGrbit.None)); }
		}

		protected readonly IDictionary<string, JET_COLUMNID> identityColumns;
		private Table details;
		protected Table Details
		{
			get { return details ?? (details = new Table(session, dbid, "details", OpenTableGrbit.None)); }
		}

		private readonly IList<Action> onCommitActions = new List<Action>();

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
			IDictionary<string, JET_COLUMNID> identityColumns,
			IDictionary<string, JET_COLUMNID> detailsColumns)
		{
			try
			{
				session = new Session(instance);
				transaction = new Transaction(session);
				Api.JetOpenDatabase(session, database, null, out dbid, OpenDatabaseGrbit.None);

				this.documentsColumns = documentsColumns;
				this.tasksColumns = tasksColumns;
				this.filesColumns = filesColumns;
				this.indexesStatsColumns = indexesStatsColumns;
				this.mappedResultsColumns = mappedResultsColumns;
				this.documentsModifiedByTransactionsColumns = documentsModifiedByTransactionsColumns;
				this.transactionsColumns = transactionsColumns;
				this.identityColumns = identityColumns;
				this.detailsColumns = detailsColumns;
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
						Api.RetrieveColumnAsString(session, Documents, documentsColumns["key"], Encoding.Unicode);
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
					var txId = Api.RetrieveColumn(session, DocumentsModifiedByTransactions, documentsModifiedByTransactionsColumns["locked_by_transaction"]);
					if (new Guid(txId) == transactionInformation.Id)
					{
						if (Api.RetrieveColumnAsBoolean(session, DocumentsModifiedByTransactions, documentsModifiedByTransactionsColumns["delete_document"]) == true)
						{
							logger.DebugFormat("Document with key '{0}' was deleted in transaction: {1}", key, transactionInformation.Id);
							return null;
						}
						data = Api.RetrieveColumn(session, DocumentsModifiedByTransactions, documentsModifiedByTransactionsColumns["data"]);
						logger.DebugFormat("Document with key '{0}' was found in transaction: {1}", key, transactionInformation.Id);
						var etag = new Guid(Api.RetrieveColumn(session, DocumentsModifiedByTransactions, documentsModifiedByTransactionsColumns["etag"]));
						return new JsonDocument
						{
							Data = data,
							Etag = etag,
							Key = Api.RetrieveColumnAsString(session, DocumentsModifiedByTransactions, documentsModifiedByTransactionsColumns["key"], Encoding.Unicode),
							Metadata = JsonCache.ParseMetadata(etag, Api.RetrieveColumn(session, DocumentsModifiedByTransactions, documentsModifiedByTransactionsColumns["metadata"]))
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
			var existingEtag = new Guid(Api.RetrieveColumn(session, Documents, documentsColumns["etag"]));
			return new JsonDocument
			{
				Data = data,
				Etag = existingEtag,
				Key = Api.RetrieveColumnAsString(session, Documents, documentsColumns["key"], Encoding.Unicode),
				Metadata = JsonCache.ParseMetadata(existingEtag, Api.RetrieveColumn(session, Documents, documentsColumns["metadata"]))
			};
		}

		public void Commit()
		{
			transaction.Commit(CommitTransactionGrbit.None);
			foreach (var action in onCommitActions)
			{
				action();
			}
		}

		public int GetNextIdentityValue(string name)
		{
			Api.JetSetCurrentIndex(session, Identity, "by_key");
			Api.MakeKey(session, Identity, name, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, Identity, SeekGrbit.SeekEQ) == false)
			{
				using (var update = new Update(session, Identity, JET_prep.Insert))
				{
					Api.SetColumn(session, Identity, identityColumns["key"], name, Encoding.Unicode);
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
				var etag = new Guid(Api.RetrieveColumn(session, Documents, documentsColumns["etag"]));
				var doc = new JsonDocument
				{
					Key = Api.RetrieveColumnAsString(session, Documents, documentsColumns["key"], Encoding.Unicode),
					Data = data,
					Etag = etag,
					Metadata = JsonCache.ParseMetadata(etag, Api.RetrieveColumn(session, Documents, documentsColumns["metadata"]))
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
					Api.EscrowUpdate(session, Details, detailsColumns["document_count"], 1);
			}
			Guid newEtag;
			DocumentDatabase.UuidCreateSequential(out newEtag);

			using (var update = new Update(session, Documents, isUpdate ? JET_prep.Replace : JET_prep.Insert))
			{
				Api.SetColumn(session, Documents, documentsColumns["key"], key, Encoding.Unicode);
				Api.SetColumn(session, Documents, documentsColumns["data"], Encoding.UTF8.GetBytes(data.ToString()));
				Api.SetColumn(session, Documents, documentsColumns["etag"], newEtag.ToByteArray());
				Api.SetColumn(session, Documents, documentsColumns["metadata"], Encoding.UTF8.GetBytes(metadata.ToString()));

				update.Save();
			}
			onCommitActions.Add(() =>
			{
				JsonCache.RememberMetadata(newEtag, metadata);
				JsonCache.RememberDocument(newEtag, data);
			});
			logger.DebugFormat("Inserted a new document with key '{0}', update: {1}, ",
							   key, isUpdate);

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
				using (var update = new Update(session, Documents, JET_prep.Replace))
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
				Api.SetColumn(session, DocumentsModifiedByTransactions, documentsModifiedByTransactionsColumns["metadata"], Encoding.UTF8.GetBytes(metadata));
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
			Api.JetSetCurrentIndex(session, DocumentsModifiedByTransactions, "by_key");
			Api.MakeKey(session, DocumentsModifiedByTransactions, key, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, DocumentsModifiedByTransactions, SeekGrbit.SeekEQ) == false)
				return;
			byte[] docTxId = Api.RetrieveColumn(session, DocumentsModifiedByTransactions, documentsModifiedByTransactionsColumns["locked_by_transaction"]);
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
				Api.SetColumn(session, Transactions, transactionsColumns["tx_id"], transactionInformation.Id.ToByteArray());
				Api.SetColumn(session, Transactions, transactionsColumns["timeout"],
							  DateTime.UtcNow + transactionInformation.Timeout);
				update.Save();
			}
		}

		public void DeleteDocument(string key, Guid? etag)
		{
			if (Api.TryMoveFirst(session, Details))
				Api.EscrowUpdate(session, Details, detailsColumns["document_count"], -1);
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
					Api.RetrieveColumn(session, Documents, documentsColumns["data"]));
				Api.SetColumn(session, DocumentsModifiedByTransactions, documentsModifiedByTransactionsColumns["etag"], newEtag.ToByteArray());
				Api.SetColumn(session, DocumentsModifiedByTransactions, documentsModifiedByTransactionsColumns["metadata"],
					Api.RetrieveColumnAsString(session, Documents, documentsColumns["metadata"], Encoding.Unicode), Encoding.Unicode);
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
			Api.MakeKey(session, Transactions, txId, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, Transactions, SeekGrbit.SeekEQ) == false)
			{
				//This is a bug, probably... because it means that we have a missing
				// transaction, we are going to reset it
				ResetTransactionOnCurrentDocument();
				return;
			}
			var timeout = Api.RetrieveColumnAsDateTime(session, Transactions, transactionsColumns["timeout"]);
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
					Data = Api.RetrieveColumn(session, DocumentsModifiedByTransactions, documentsModifiedByTransactionsColumns["data"]),
					Delete = Api.RetrieveColumnAsBoolean(session, DocumentsModifiedByTransactions, documentsModifiedByTransactionsColumns["delete_document"]).Value,
					Etag = new Guid(Api.RetrieveColumn(session, DocumentsModifiedByTransactions, documentsModifiedByTransactionsColumns["etag"])),
					Key = Api.RetrieveColumnAsString(session, DocumentsModifiedByTransactions, documentsModifiedByTransactionsColumns["key"], Encoding.Unicode),
					Metadata = Api.RetrieveColumn(session, DocumentsModifiedByTransactions, documentsModifiedByTransactionsColumns["metadata"]),
				};
				Api.JetDelete(session, DocumentsModifiedByTransactions);
				perDocumentModified(documentInTransactionData);
			} while (Api.TryMoveNext(session, DocumentsModifiedByTransactions));
		}

		private void ResetTransactionOnCurrentDocument()
		{
			using (var update = new Update(session, Documents, JET_prep.Replace))
			{
				Api.SetColumn(session, Documents, documentsColumns["locked_by_transaction"], null);
				update.Save();
			}
		}

		public void AddTask(Task task)
		{
			if (task.SupportsMerging == false)
			{
				InsertNewTask(task);
				return;
			}

			Api.JetSetCurrentIndex(session, Tasks, "mergables_by_task_type");
			Api.MakeKey(session, Tasks, true, MakeKeyGrbit.NewKey);
			Api.MakeKey(session, Tasks, task.Index, Encoding.Unicode, MakeKeyGrbit.None);
			Api.MakeKey(session, Tasks, task.Type, Encoding.Unicode, MakeKeyGrbit.None);
			// there are no tasks matching the current one, just insert it
			if (Api.TrySeek(session, Tasks, SeekGrbit.SeekEQ) == false)
			{
				InsertNewTask(task);
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
					var taskId = Api.RetrieveColumnAsInt32(session, Tasks, tasksColumns["id"]);
					var taskAsString = Api.RetrieveColumnAsString(session, Tasks, tasksColumns["task"], Encoding.Unicode);
					var existingTask = TaskCache.ParseTask(taskId.Value, taskAsString);
					if (existingTask.TryMerge(task) == false)
						continue;

					using (var update = new Update(session, Tasks, JET_prep.Replace))
					{
						Api.SetColumn(session, Tasks, tasksColumns["task"], existingTask.AsString(), Encoding.Unicode);
						Api.SetColumn(session, Tasks, tasksColumns["supports_merging"], existingTask.SupportsMerging);
						update.Save();
					}
					onCommitActions.Add(() => TaskCache.RememberTask(taskId.Value, task));
				}
				catch (EsentErrorException e)
				{
					if (e.Error == JET_err.WriteConflict)
						continue;
					throw;
				}
				return;
			} while (Api.TryMoveNext(session, Tasks));
			// nothing that we could merge into, need to insert a new one
			InsertNewTask(task);
		}

		private void InsertNewTask(Task task)
		{
			int actualBookmarkSize;
			var bookmark = new byte[SystemParameters.BookmarkMost];
			using (var update = new Update(session, Tasks, JET_prep.Insert))
			{
				Api.SetColumn(session, Tasks, tasksColumns["task"], task.AsString(), Encoding.Unicode);
				Api.SetColumn(session, Tasks, tasksColumns["for_index"], task.Index, Encoding.Unicode);
				Api.SetColumn(session, Tasks, tasksColumns["task_type"], task.Type, Encoding.Unicode);
				Api.SetColumn(session, Tasks, tasksColumns["supports_merging"], task.SupportsMerging);

				update.Save(bookmark, bookmark.Length, out actualBookmarkSize);
			}
			Api.JetGotoBookmark(session, Tasks, bookmark, actualBookmarkSize);
			var taskId = Api.RetrieveColumnAsInt32(session, Tasks, tasksColumns["id"]);
			onCommitActions.Add(() => TaskCache.RememberTask(taskId.Value, task));
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
			else
			{
				if (Api.TryMoveFirst(session, Details))
					Api.EscrowUpdate(session, Details, detailsColumns["attachment_count"], 1);
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
			if (Api.TryMoveFirst(session, Details))
				Api.EscrowUpdate(session, Details, detailsColumns["attachment_count"], -1);
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
				var first = (int)Api.RetrieveColumnAsInt32(session, Tasks,tasksColumns["id"]);
				if (Api.TryMoveLast(session, Tasks) == false)
					return 0;
				var last = (int)Api.RetrieveColumnAsInt32(session, Tasks, tasksColumns["id"]);
				return last - first;
			}
		}

		public string GetFirstTask()
		{
			Api.MoveBeforeFirst(session, Tasks);
			while (Api.TryMoveNext(session, Tasks))
			{
				var task = Api.RetrieveColumnAsString(session, Tasks, tasksColumns["task"], Encoding.Unicode);
				try
				{
					Api.JetDelete(session, Tasks);
				}
				catch (EsentErrorException e)
				{
					if (e.Error != JET_err.WriteConflict)
						throw;
				}

				return task;
			}
			return null;
		}

		public int GetDocumentsCount()
		{
			if (Api.TryMoveFirst(session, Details))
				return Api.RetrieveColumnAsInt32(session, Details, detailsColumns["document_count"]).Value;
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

			Guid etag;
			DocumentDatabase.UuidCreateSequential(out etag);

			using (var update = new Update(session, MappedResults, isUpdate ? JET_prep.Replace : JET_prep.Insert))
			{
				Api.SetColumn(session, MappedResults, mappedResultsColumns["view"], view, Encoding.Unicode);
				Api.SetColumn(session, MappedResults, mappedResultsColumns["document_key"], docId, Encoding.Unicode);
				Api.SetColumn(session, MappedResults, mappedResultsColumns["reduce_key"], reduceKey, Encoding.Unicode);
				Api.SetColumn(session, MappedResults, mappedResultsColumns["data"], Encoding.UTF8.GetBytes(data));
				Api.SetColumn(session, MappedResults, mappedResultsColumns["etag"], etag.ToByteArray());

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
				var bytes = Api.RetrieveColumn(session, MappedResults, mappedResultsColumns["data"]);
				var etag = new Guid(Api.RetrieveColumn(session, MappedResults, mappedResultsColumns["etag"]));
				yield return JsonCache.ParseDocument(etag, bytes);
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
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using log4net;
using Microsoft.Isam.Esent.Interop;
using Raven.Database;
using Raven.Database.Plugins;
using Raven.Database.Json;
using Raven.Database.Storage;
using Raven.Database.Storage.StorageActions;
using Raven.Database.Extensions;
using Raven.Http;

namespace Raven.Storage.Esent.StorageActions
{
	[CLSCompliant(false)]
	public partial class DocumentStorageActions : IDisposable, IGeneralStorageActions
	{
		public event Action OnCommit = delegate { }; 
		private readonly TableColumnsCache tableColumnsCache;
		private readonly IEnumerable<AbstractDocumentCodec> documentCodecs;
		protected readonly JET_DBID dbid;

		protected readonly ILog logger = LogManager.GetLogger(typeof(DocumentStorageActions));
		protected readonly Session session;
		private readonly Transaction transaction;

		public Session Session
		{
			get { return session; }
		}

		[CLSCompliant(false)]
		[DebuggerHidden, DebuggerNonUserCode, DebuggerStepThrough]
		public DocumentStorageActions(JET_INSTANCE instance, string database, TableColumnsCache tableColumnsCache, IEnumerable<AbstractDocumentCodec> documentCodecs)
		{
			this.tableColumnsCache = tableColumnsCache;
			this.documentCodecs = documentCodecs;
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

		#region IDisposable Members

		[DebuggerHidden, DebuggerNonUserCode, DebuggerStepThrough]
		public void Dispose()
		{
            if(queue != null)
                queue.Dispose();

			if(directories != null)
				directories.Dispose();

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

		public void Commit(CommitTransactionGrbit txMode)
		{
			transaction.Commit(txMode);
			OnCommit();
		}

		public long GetNextIdentityValue(string name)
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


		public void RollbackTransaction(Guid txId)
		{
			CompleteTransaction(txId, doc =>
			{
				Api.JetSetCurrentIndex(session,Documents,"by_key");
				Api.MakeKey(session, Documents, doc.Key, Encoding.Unicode, MakeKeyGrbit.NewKey);
				if (Api.TrySeek(session, Documents, SeekGrbit.SeekEQ))
				{
					ResetTransactionOnCurrentDocument();
				}
			});
		}

		public void ModifyTransactionId(Guid fromTxId, Guid toTxId, TimeSpan timeout)
		{
			var transactionInformation = new TransactionInformation
			{
				Id = toTxId,
				Timeout = timeout
			};
			EnsureTransactionExists(transactionInformation);
			CompleteTransaction(fromTxId, doc =>
			{
				Api.JetSetCurrentIndex(session, Documents, "by_key");
				Api.MakeKey(session, Documents, doc.Key, Encoding.Unicode, MakeKeyGrbit.NewKey);
				if (Api.TrySeek(session, Documents, SeekGrbit.SeekEQ))
				{
					using (var update = new Update(session, Documents, JET_prep.Replace))
					{
						Api.SetColumn(session, Documents, tableColumnsCache.DocumentsColumns["locked_by_transaction"], toTxId);
						update.Save();
					}
				}

				if (doc.Delete)
					DeleteDocumentInTransaction(transactionInformation, doc.Key, null);
				else
					AddDocumentInTransaction(doc.Key, null, doc.Data, doc.Metadata, transactionInformation);
			});
		}
		
		public IEnumerable<Guid> GetTransactionIds()
		{
			Api.MoveBeforeFirst(session, Transactions);
			while(Api.TryMoveNext(session, Transactions))
			{
				var guid = Api.RetrieveColumnAsGuid(session, Transactions, tableColumnsCache.TransactionsColumns["tx_id"]);
				yield return (Guid)guid;
			}
		}

		public void CompleteTransaction(Guid txId, Action<DocumentInTransactionData> perDocumentModified)
		{
			Api.JetSetCurrentIndex(session, Transactions, "by_tx_id");
			Api.MakeKey(session, Transactions, txId, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, Transactions, SeekGrbit.SeekEQ))
				Api.JetDelete(session, Transactions);

			Api.JetSetCurrentIndex(session, DocumentsModifiedByTransactions, "by_tx");
			Api.MakeKey(session, DocumentsModifiedByTransactions, txId, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, DocumentsModifiedByTransactions, SeekGrbit.SeekEQ) == false)
				return;
			Api.MakeKey(session, DocumentsModifiedByTransactions, txId, MakeKeyGrbit.NewKey);
			Api.JetSetIndexRange(session, DocumentsModifiedByTransactions, SetIndexRangeGrbit.RangeInclusive | SetIndexRangeGrbit.RangeUpperLimit);
			var bookmarksToDelete = new List<byte[]>();
			var documentsInTransaction = new List<DocumentInTransactionData>();
			do
			{
				// esent index ranges are approximate, and we need to check them ourselves as well
				if (Api.RetrieveColumnAsGuid(session, DocumentsModifiedByTransactions, tableColumnsCache.DocumentsModifiedByTransactionsColumns["locked_by_transaction"]) != txId)
					continue;
				var data = Api.RetrieveColumn(session, DocumentsModifiedByTransactions, tableColumnsCache.DocumentsModifiedByTransactionsColumns["data"]);
				var metadata = Api.RetrieveColumn(session, DocumentsModifiedByTransactions, tableColumnsCache.DocumentsModifiedByTransactionsColumns["metadata"]);
				var key = Api.RetrieveColumnAsString(session, DocumentsModifiedByTransactions, tableColumnsCache.DocumentsModifiedByTransactionsColumns["key"], Encoding.Unicode);

				if(data != null && metadata != null)
				{
					var metadataAsJson = metadata.ToJObject();
					data = documentCodecs.Aggregate(data, (bytes, codec) => codec.Decode(key, metadataAsJson, bytes));
				}

				documentsInTransaction.Add(new DocumentInTransactionData
				{
					Data = data.ToJObject(),
					Delete =
						Api.RetrieveColumnAsBoolean(session, DocumentsModifiedByTransactions,
						                            tableColumnsCache.DocumentsModifiedByTransactionsColumns["delete_document"]).Value,
					Etag = Api.RetrieveColumn(session, DocumentsModifiedByTransactions,
						                            tableColumnsCache.DocumentsModifiedByTransactionsColumns["etag"]).TransfromToGuidWithProperSorting(),
					Key = key,
					Metadata = metadata.ToJObject(),
				});
				bookmarksToDelete.Add(Api.GetBookmark(session, DocumentsModifiedByTransactions));
			} while (Api.TryMoveNext(session, DocumentsModifiedByTransactions));

			foreach (var bookmark in bookmarksToDelete)
			{
				Api.JetGotoBookmark(session, DocumentsModifiedByTransactions, bookmark, bookmark.Length);
				Api.JetDelete(session, DocumentsModifiedByTransactions);
			}
			foreach (var documentInTransactionData in documentsInTransaction)
			{
				perDocumentModified(documentInTransactionData);
			}
		}

		private void ResetTransactionOnCurrentDocument()
		{
			using (var update = new Update(session, Documents, JET_prep.Replace))
			{
				Api.SetColumn(session, Documents, tableColumnsCache.DocumentsColumns["locked_by_transaction"], null);
				update.Save();
			}
		}


	}

	
}

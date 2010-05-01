using System;
using System.Diagnostics;
using System.Text;
using log4net;
using Microsoft.Isam.Esent.Interop;

namespace Raven.Database.Storage
{
	[CLSCompliant(false)]
	public partial class DocumentStorageActions : IDisposable
	{
		public event Action OnCommit = delegate { }; 
		private readonly TableColumnsCache tableColumnsCache;
		protected readonly JET_DBID dbid;

		protected readonly ILog logger = LogManager.GetLogger(typeof(DocumentStorageActions));
		protected readonly Session session;
		private readonly Transaction transaction;
		

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


	}

	
}
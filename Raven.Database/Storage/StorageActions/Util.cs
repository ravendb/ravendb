using System;
using System.Text;
using Microsoft.Isam.Esent.Interop;
using Raven.Database.Exceptions;

namespace Raven.Database.Storage
{
	public partial class DocumentStorageActions
	{

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
	}
}
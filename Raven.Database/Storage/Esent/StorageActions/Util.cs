//-----------------------------------------------------------------------
// <copyright file="Util.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Text;
using Microsoft.Isam.Esent.Interop;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Database;
using Raven.Database.Extensions;

namespace Raven.Storage.Esent.StorageActions
{
	public partial class DocumentStorageActions
	{

		private Etag EnsureDocumentEtagMatch(string key, Etag etag, string method)
		{
			var existingEtag = Etag.Parse(Api.RetrieveColumn(session, Documents, tableColumnsCache.DocumentsColumns["etag"]));
			if (existingEtag != etag && etag != null)
			{
				if(etag == Etag.InvalidEtag)
				{
					var metadata = Api.RetrieveColumn(session, Documents, tableColumnsCache.DocumentsColumns["metadata"]).ToJObject();
					if(metadata.ContainsKey(Constants.RavenDeleteMarker) && 
						metadata.Value<bool>(Constants.RavenDeleteMarker))
					{
						return existingEtag;
					}
				}

				throw new ConcurrencyException(method + " attempted on document '" + key +
											   "' using a non current etag")
				{
					ActualETag = existingEtag,
					ExpectedETag = etag
				};
			}
			return existingEtag;
		}

		private void EnsureDocumentEtagMatchInTransaction(string key, Etag etag)
		{
			Api.JetSetCurrentIndex(session, DocumentsModifiedByTransactions, "by_key");
			Api.MakeKey(session, DocumentsModifiedByTransactions, key, Encoding.Unicode, MakeKeyGrbit.NewKey);
			Etag existingEtag;
			if (Api.TrySeek(session, DocumentsModifiedByTransactions, SeekGrbit.SeekEQ))
			{
				existingEtag = Etag.Parse(Api.RetrieveColumn(session, DocumentsModifiedByTransactions, tableColumnsCache.DocumentsModifiedByTransactionsColumns["etag"]));
			}
			else
			{
				existingEtag = Etag.Parse(Api.RetrieveColumn(session, Documents, tableColumnsCache.DocumentsColumns["etag"]));
			}
			if (existingEtag != etag && etag != null)
			{
				throw new ConcurrencyException("PUT attempted on document '" + key +
											   "' using a non current etag")
				{
					ActualETag = existingEtag,
					ExpectedETag = etag
				};
			}
		}



		private void EnsureDocumentIsNotCreatedInAnotherTransaction(string key, Guid txId)
		{
			if(IsDocumentModifiedInsideTransaction(key) == false)
				return;
			byte[] docTxId = Api.RetrieveColumn(session, DocumentsModifiedByTransactions, tableColumnsCache.DocumentsModifiedByTransactionsColumns["locked_by_transaction"]);
			if (new Guid(docTxId) != txId)
			{
				var retrieveColumnAsInt64 = Api.RetrieveColumnAsInt64(session, Transactions, tableColumnsCache.TransactionsColumns["timeout"]);
				var timeout = DateTime.FromBinary(retrieveColumnAsInt64.Value);
				if (SystemTime.UtcNow > timeout)// the timeout for the transaction has passed
				{
					RollbackTransaction(new Guid(docTxId));
					return;
				}

				throw new ConcurrencyException("A document with key: '" + key + "' is currently created in another transaction");
			}
		}

		private bool IsDocumentModifiedInsideTransaction(string key)
		{
			Api.JetSetCurrentIndex(session, DocumentsModifiedByTransactions, "by_key");
			Api.MakeKey(session, DocumentsModifiedByTransactions, key, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if(Api.TrySeek(session, DocumentsModifiedByTransactions, SeekGrbit.SeekEQ) == false)
				return false;
			var txId = Api.RetrieveColumn(session, DocumentsModifiedByTransactions,
										  tableColumnsCache.DocumentsModifiedByTransactionsColumns["locked_by_transaction"]);

			Api.JetSetCurrentIndex(session, Transactions, "by_tx_id");
			Api.MakeKey(session, Transactions, txId, MakeKeyGrbit.NewKey);
			if(Api.TrySeek(session, Transactions, SeekGrbit.SeekEQ) == false)
				return false;
			
			var timeout = Api.RetrieveColumnAsInt64(session, Transactions, tableColumnsCache.TransactionsColumns["timeout"]);
			return SystemTime.UtcNow < DateTime.FromBinary(timeout.Value);
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
							  (SystemTime.UtcNow + transactionInformation.Timeout).ToBinary());
				try
				{
					update.Save();
				}
				catch (EsentKeyDuplicateException)
				{
					// someone else might have created this record in a separate thread
					// that is fine from our point of view
				}
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
			var timeout = Api.RetrieveColumnAsInt64(session, Transactions, tableColumnsCache.TransactionsColumns["timeout"]);
			if (SystemTime.UtcNow > DateTime.FromBinary(timeout.Value))// the timeout for the transaction has passed
			{
				var bookmark = Api.GetBookmark(session, Documents);
				RollbackTransaction(guid);
				Api.JetGotoBookmark(session, Documents, bookmark, bookmark.Length);
				return;
			}
			throw new ConcurrencyException("Document '" + key + "' is locked by transaction: " + guid);
		}
	}
}

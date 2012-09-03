using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Database.Impl;
using Raven.Json.Linq;

namespace Raven.Database.Storage.RAM
{
	class RamTransactionStorageActions : ITransactionStorageActions
	{
		private readonly RamState state;
		private readonly IUuidGenerator generator;

		public RamTransactionStorageActions(RamState state, IUuidGenerator generator)
		{
			this.state = state;
			this.generator = generator;
		}

		public Guid AddDocumentInTransaction(string key, Guid? etag, RavenJObject data, RavenJObject metadata, TransactionInformation transactionInformation)
		{
			var doc = state.Documents.FirstOrDefault(document => document.Key == key);

			if(doc != null)
			{
				EnsureNotLockedByTransaction(key, transactionInformation.Id);
				EnsureDocumentEtagMatchInTransaction(key, etag);
				//using (var update = new Update(session, Documents, JET_prep.Replace))
				//{
				//	Api.SetColumn(session, Documents, tableColumnsCache.DocumentsColumns["locked_by_transaction"], transactionInformation.Id.ToByteArray());
				//	update.Save();
				//}
			}
			else
			{
				EnsureDocumentIsNotCreatedInAnotherTransaction(key, transactionInformation.Id);
			}

			EnsureTransactionExists(transactionInformation);
			Guid newEtag = generator.CreateSequentialUuid();

			var transaction = state.Transactions.GetOrAdd(key);
			transaction.Key = transactionInformation.Id;
			transaction.TimeOut = SystemTime.UtcNow + transactionInformation.Timeout;
			transaction.Document = new JsonDocument
			{
				Key = key,
				DataAsJson = data,
				Etag = etag,
				LastModified = SystemTime.UtcNow,
				Metadata = metadata,
			};

			//Api.JetSetCurrentIndex(session, DocumentsModifiedByTransactions, "by_key");
			//Api.MakeKey(session, DocumentsModifiedByTransactions, key, Encoding.Unicode, MakeKeyGrbit.NewKey);
			//var isUpdateInTransaction = Api.TrySeek(session, DocumentsModifiedByTransactions, SeekGrbit.SeekEQ);

			//using (var update = new Update(session, DocumentsModifiedByTransactions, isUpdateInTransaction ? JET_prep.Replace : JET_prep.Insert))
			//{
			//	Api.SetColumn(session, DocumentsModifiedByTransactions, tableColumnsCache.DocumentsModifiedByTransactionsColumns["key"], key, Encoding.Unicode);

			//	using (Stream stream = new BufferedStream(new ColumnStream(session, DocumentsModifiedByTransactions, tableColumnsCache.DocumentsModifiedByTransactionsColumns["data"])))
			//	using (var finalStream = documentCodecs.Aggregate(stream, (current, codec) => codec.Encode(key, data, metadata, current)))
			//	{
			//		data.WriteTo(finalStream);
			//		finalStream.Flush();
			//	}
			//	Api.SetColumn(session, DocumentsModifiedByTransactions,
			//				  tableColumnsCache.DocumentsModifiedByTransactionsColumns["etag"],
			//				  newEtag.TransformToValueForEsentSorting());

			//	using (Stream stream = new BufferedStream(new ColumnStream(session, DocumentsModifiedByTransactions, tableColumnsCache.DocumentsModifiedByTransactionsColumns["metadata"])))
			//	{
			//		metadata.WriteTo(stream);
			//		stream.Flush();
			//	}

			//	Api.SetColumn(session, DocumentsModifiedByTransactions, tableColumnsCache.DocumentsModifiedByTransactionsColumns["last_modified"], SystemTime.UtcNow);
			//	Api.SetColumn(session, DocumentsModifiedByTransactions, tableColumnsCache.DocumentsModifiedByTransactionsColumns["delete_document"], false);
			//	Api.SetColumn(session, DocumentsModifiedByTransactions, tableColumnsCache.DocumentsModifiedByTransactionsColumns["locked_by_transaction"], transactionInformation.Id.ToByteArray());

			//	update.Save();
			//}
			//logger.Debug("Inserted a new document with key '{0}', update: {1}, in transaction: {2}",
			//				   key, isUpdate, transactionInformation.Id);

			return newEtag;
		}

		public bool DeleteDocumentInTransaction(TransactionInformation transactionInformation, string key, Guid? etag)
		{
			var transaction = state.Transactions.FirstOrDefault(pair => pair.Key == key);

			if (transaction.Value == null)
				return false;

			EnsureNotLockedByTransaction(key, transactionInformation.Id);
			EnsureDocumentEtagMatchInTransaction(key, etag);

			//using (var update = new Update(session, Documents, JET_prep.Replace))
			//{
			//	Api.SetColumn(session, Documents, tableColumnsCache.DocumentsColumns["locked_by_transaction"], transactionInformation.Id.ToByteArray());
			//	update.Save();
			//}

			EnsureTransactionExists(transactionInformation);

			Guid newEtag = generator.CreateSequentialUuid();

			//Api.JetSetCurrentIndex(session, DocumentsModifiedByTransactions, "by_key");
			//Api.MakeKey(session, DocumentsModifiedByTransactions, key, Encoding.Unicode, MakeKeyGrbit.NewKey);
			//var isUpdateInTransaction = Api.TrySeek(session, DocumentsModifiedByTransactions, SeekGrbit.SeekEQ);

			//using (var update = new Update(session, DocumentsModifiedByTransactions, isUpdateInTransaction ? JET_prep.Replace : JET_prep.Insert))
			//{
			//	Api.SetColumn(session, DocumentsModifiedByTransactions, tableColumnsCache.DocumentsModifiedByTransactionsColumns["key"], key, Encoding.Unicode);
			//	Api.SetColumn(session, DocumentsModifiedByTransactions, tableColumnsCache.DocumentsModifiedByTransactionsColumns["data"],
			//		Api.RetrieveColumn(session, Documents, tableColumnsCache.DocumentsColumns["data"]));
			//	Api.SetColumn(session, DocumentsModifiedByTransactions,
			//				  tableColumnsCache.DocumentsModifiedByTransactionsColumns["etag"],
			//				  newEtag.TransformToValueForEsentSorting());
			//	Api.SetColumn(session, DocumentsModifiedByTransactions, tableColumnsCache.DocumentsModifiedByTransactionsColumns["last_modified"],
			//		Api.RetrieveColumnAsDateTime(session, Documents, tableColumnsCache.DocumentsColumns["last_modified"]).Value);
			//	Api.SetColumn(session, DocumentsModifiedByTransactions, tableColumnsCache.DocumentsModifiedByTransactionsColumns["metadata"],
			//		Api.RetrieveColumn(session, Documents, tableColumnsCache.DocumentsColumns["metadata"]));
			//	Api.SetColumn(session, DocumentsModifiedByTransactions, tableColumnsCache.DocumentsModifiedByTransactionsColumns["delete_document"], true);
			//	Api.SetColumn(session, DocumentsModifiedByTransactions, tableColumnsCache.DocumentsModifiedByTransactionsColumns["locked_by_transaction"], transactionInformation.Id.ToByteArray());

			//	update.Save();
			//}

			return true;
		}

		public void RollbackTransaction(Guid txId)
		{
			CompleteTransaction(txId, doc =>
			{
				var transaction = state.Transactions.FirstOrDefault(pair => pair.Key == doc.Key);

				//Api.MakeKey(session, Documents, doc.Key, Encoding.Unicode, MakeKeyGrbit.NewKey);
				//if (Api.TrySeek(session, Documents, SeekGrbit.SeekEQ))
				//{
				//	ResetTransactionOnCurrentDocument();
				//}
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
				//Api.JetSetCurrentIndex(session, Documents, "by_key");
				//Api.MakeKey(session, Documents, doc.Key, Encoding.Unicode, MakeKeyGrbit.NewKey);
				//if (Api.TrySeek(session, Documents, SeekGrbit.SeekEQ))
				//{
				//	using (var update = new Update(session, Documents, JET_prep.Replace))
				//	{
				//		Api.SetColumn(session, Documents, tableColumnsCache.DocumentsColumns["locked_by_transaction"], toTxId);
				//		update.Save();
				//	}
				//}

				if (doc.Delete)
					DeleteDocumentInTransaction(transactionInformation, doc.Key, null);
				else
					AddDocumentInTransaction(doc.Key, null, doc.Data, doc.Metadata, transactionInformation);
			});
		}

		public void CompleteTransaction(Guid txId, Action<DocumentInTransactionData> perDocumentModified)
		{
			throw new NotImplementedException();
		}

		public IEnumerable<Guid> GetTransactionIds()
		{
			return state.Transactions.Select(pair => pair.Value.Key);
		}

		public bool TransactionExists(Guid txId)
		{
			return state.Transactions.Any(pair => pair.Value.Key == txId);
		}

		private void EnsureNotLockedByTransaction(string key, Guid? currentTransaction)
		{
			var transaction = state.Transactions.FirstOrDefault(pair => pair.Key == key);

			if (transaction.Value == null)
			{
				return;
			}

			if (currentTransaction != null && transaction.Value.Key == currentTransaction.Value)
				return;

			//Api.JetSetCurrentIndex(session, Transactions, "by_tx_id");
			//Api.MakeKey(session, Transactions, txId, MakeKeyGrbit.NewKey);
			//if (Api.TrySeek(session, Transactions, SeekGrbit.SeekEQ) == false)
			//{
			//	//This is a bug, probably... because it means that we have a missing
			//	// transaction, we are going to reset it
			//	ResetTransactionOnCurrentDocument();
			//	return;
			//}

			if (SystemTime.UtcNow > transaction.Value.TimeOut)// the timeout for the transaction has passed
			{
				RollbackTransaction(transaction.Value.Key);
				return;
			}

			throw new ConcurrencyException("Document '" + key + "' is locked by transaction: " + transaction.Value.Key);
		}

		private void EnsureDocumentEtagMatchInTransaction(string key, Guid? etag)
		{
			var transaction = state.Transactions.FirstOrDefault(pair => pair.Key == key);

			Guid existingEtag = transaction.Value.Key;


			if(transaction.Value.Command == TransationCommand.Delete)
				return; // we ignore etags on deleted documents

			//if (Api.TrySeek(session, DocumentsModifiedByTransactions, SeekGrbit.SeekEQ))
			//{

			//	existingEtag = Api.RetrieveColumn(session, DocumentsModifiedByTransactions, tableColumnsCache.DocumentsModifiedByTransactionsColumns["etag"]).TransfromToGuidWithProperSorting();
			//}
			//else
			//{
			//	existingEtag = Api.RetrieveColumn(session, Documents, tableColumnsCache.DocumentsColumns["etag"]).TransfromToGuidWithProperSorting();
			//}


			if (existingEtag != etag && etag != null)
			{
				throw new ConcurrencyException("PUT attempted on document '" + key +
											   "' using a non current etag")
				{
					ActualETag = existingEtag,
					ExpectedETag = etag.Value
				};
			}
		}

		private void EnsureDocumentIsNotCreatedInAnotherTransaction(string key, Guid txId)
		{
			if (IsDocumentModifiedInsideTransaction(key) == false)
				return;

			var transaction = state.Transactions.FirstOrDefault(pair => pair.Key == key);

			if (transaction.Value.Key != txId)
			{
				var timeout = transaction.Value.TimeOut;
				if (SystemTime.UtcNow > timeout)// the timeout for the transaction has passed
				{
					RollbackTransaction(transaction.Value.Key);
					return;
				}

				throw new ConcurrencyException("A document with key: '" + key + "' is currently created in another transaction");
			}
		}

		private bool IsDocumentModifiedInsideTransaction(string key)
		{
			var transaction = state.Transactions.FirstOrDefault(pair => pair.Key == key);
			if (transaction.Value == null)
				return false;


			//var txId = transaction.Value.Key;

			//Api.JetSetCurrentIndex(session, Transactions, "by_tx_id");
			//Api.MakeKey(session, Transactions, txId, MakeKeyGrbit.NewKey);
			//if (Api.TrySeek(session, Transactions, SeekGrbit.SeekEQ) == false)
			//	return false;

			var timeout = transaction.Value.TimeOut;
			return SystemTime.UtcNow < timeout;
		}

		private void EnsureTransactionExists(TransactionInformation transactionInformation)
		{
			//Api.JetSetCurrentIndex(session, Transactions, "by_tx_id");
			//Api.MakeKey(session, Transactions, transactionInformation.Id.ToByteArray(), MakeKeyGrbit.NewKey);
			//var isUpdate = Api.TrySeek(session, Transactions, SeekGrbit.SeekEQ);
			//using (var update = new Update(session, Transactions, isUpdate ? JET_prep.Replace : JET_prep.Insert))
			//{
			//	Api.SetColumn(session, Transactions, tableColumnsCache.TransactionsColumns["tx_id"], transactionInformation.Id.ToByteArray());
			//	Api.SetColumn(session, Transactions, tableColumnsCache.TransactionsColumns["timeout"],
			//				  SystemTime.UtcNow + transactionInformation.Timeout);
			//	try
			//	{
			//		update.Save();
			//	}
			//	catch (EsentKeyDuplicateException)
			//	{
			//		// someone else might ahvehave created this record in a separate thread
			//		// that is fine from our point of view
			//	}
			//}
		}
	}
}

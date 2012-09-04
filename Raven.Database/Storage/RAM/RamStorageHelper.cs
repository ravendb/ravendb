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
	public class RamStorageHelper
	{
		private readonly RamState state;

		public RamStorageHelper(RamState state)
		{
			this.state = state;
		}

		public void EnsureDocumentEtagMatchInTransaction(string key, Guid? etag)
		{
			var documentsModifiedByTransation = state.DocumentsModifiedByTransations.GetOrDefault(key);
			var doc = state.Documents.GetOrDefault(key);

			if (doc == null)
				return; // no such document

			Guid existingEtag;
			if (documentsModifiedByTransation != null)
			{
				if (documentsModifiedByTransation.DeleteDocument)
					return; // we ignore etags on deleted documents
				if (documentsModifiedByTransation.Document.Etag == null)
					existingEtag = Guid.NewGuid();
				else
					existingEtag = (Guid)documentsModifiedByTransation.Document.Etag;
			}
			else
			{
				if (doc.Document.Etag == null)
					existingEtag = Guid.NewGuid();
				else
					existingEtag = (Guid)doc.Document.Etag;
			}

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

		public void EnsureNotLockedByTransaction(string key, Guid? currentTransaction)
		{
			var doc = state.Documents.GetOrDefault(key);
			if (doc == null)
				return;
			var txId = doc.LockByTransaction;

			if (txId == null)
				return;

			if (currentTransaction != null && txId == currentTransaction.Value)
				return;

			var transaction = state.Transactions.GetOrDefault((Guid)txId);

			if (transaction == null)
			{
				//This is a bug, probably... because it means that we have a missing
				// transaction, we are going to reset it
				ResetTransactionOnDocument(key);
				return;
			}

			var timeout = transaction.TimeOut;
			if (SystemTime.UtcNow > timeout)// the timeout for the transaction has passed
			{
				RollbackTransaction((Guid)txId);
				return;
			}

			throw new ConcurrencyException("Document '" + key + "' is locked by transacton: " + txId);
		}

		public void EnsureDocumentIsNotCreatedInAnotherTransaction(string key, Guid txId)
		{
			if (IsDocumentModifiedInsideTransaction(key) == false)
				return;

			var doc = state.DocumentsModifiedByTransations.GetOrDefault(key);
			if (doc == null)
				return;

			var docTxId = doc.LockByTransaction;
			if (docTxId == null)
				return;

			var transaction = state.Transactions.GetOrDefault((Guid)docTxId);

			if (docTxId != txId)
			{
				var timeout = transaction.TimeOut;
				if (SystemTime.UtcNow > timeout)// the timeout for the transaction has passed
				{
					RollbackTransaction((Guid)docTxId);
					return;
				}

				throw new ConcurrencyException("A document with key: '" + key + "' is currently created in another transaction");
			}
		}

		public bool IsDocumentModifiedInsideTransaction(string key)
		{
			var doc = state.DocumentsModifiedByTransations.GetOrDefault(key);

			if (doc == null)
				return false;

			var txId = doc.LockByTransaction;

			if (txId == null)
				return false;

			var transaction = state.Transactions.GetOrDefault((Guid)txId);

			if (transaction == null)
				return false;

			var timeout = transaction.TimeOut;
			return SystemTime.UtcNow < timeout;
		}

		public void EnsureTransactionExists(TransactionInformation transactionInformation)
		{
			state.Transactions.Set(transactionInformation.Id, new Transaction
			{
				Key = transactionInformation.Id,
				TimeOut = SystemTime.UtcNow + transactionInformation.Timeout
			});
		}

		public void ResetTransactionOnDocument(string key)
		{
			var doc = state.Documents.GetOrDefault(key);
			if (doc == null)
				return;
			state.Documents.Set(key, new DocuementWrapper
			{
				Document = doc.Document
			});
		}

		public void RollbackTransaction(Guid txId)
		{
			CompleteTransaction(txId, doc =>
			{
				var document = state.Documents.GetOrDefault(doc.Key);

				if (document != null)
				{
					ResetTransactionOnDocument(doc.Key);
				}
			});
		}

		public void CompleteTransaction(Guid txId, Action<DocumentInTransactionData> perDocumentModified)
		{
			var transaction = state.Transactions.GetOrDefault(txId);

			if (transaction != null)
				state.Transactions.Remove(txId);

			var docs = state.DocumentsModifiedByTransations.Where(pair => pair.Value.LockByTransaction == txId).ToList();

			if (docs.Count == 0)
				return;

			var documentsInTransaction = new List<DocumentInTransactionData>();

			foreach (var doc in docs)
			{
				documentsInTransaction.Add(new DocumentInTransactionData
				{
					Data = doc.Value.Document.DataAsJson,
					Delete = doc.Value.DeleteDocument,
					Etag = (Guid)doc.Value.Document.Etag,
					Key = doc.Value.Document.Key,
					Metadata = doc.Value.Document.Metadata,
				});
			}

			foreach (var documentInTransactionData in documentsInTransaction)
			{
				perDocumentModified(documentInTransactionData);
			}
		}

		public Guid EnsureDocumentEtagMatch(string key, Guid? etag, string method)
		{
			var doc = state.Documents.GetOrDefault(key);
			var existingEtag = doc.Document.Etag ?? Guid.NewGuid();
			if (existingEtag != etag && etag != null)
			{
				if (etag == Guid.Empty)
				{
					var metadata = doc.Document.Metadata;
					if (metadata.ContainsKey(Constants.RavenDeleteMarker) &&
						metadata.Value<bool>(Constants.RavenDeleteMarker))
					{
						return existingEtag;
					}
				}

				throw new ConcurrencyException(method + " attempted on document '" + key +
											   "' using a non current etag")
				{
					ActualETag = existingEtag,
					ExpectedETag = etag.Value
				};
			}
			return existingEtag;
		}
	}
}
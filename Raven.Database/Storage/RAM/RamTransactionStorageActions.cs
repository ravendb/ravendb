using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Database.Impl;
using Raven.Json.Linq;

namespace Raven.Database.Storage.RAM
{
	class RamTransactionStorageActions : ITransactionStorageActions
	{
		private readonly RamState state;
		private readonly IUuidGenerator generator;
		private readonly RamStorageHelper helper;

		public RamTransactionStorageActions(RamState state, IUuidGenerator generator)
		{
			this.state = state;
			this.generator = generator;
			helper = new RamStorageHelper(state);
		}

		public Guid AddDocumentInTransaction(string key, Guid? etag, RavenJObject data, RavenJObject metadata, TransactionInformation transactionInformation)
		{
			var doc = state.Documents.GetOrDefault(key);

			var isUpdate = doc != null;
			if (isUpdate)
			{
				helper.EnsureNotLockedByTransaction(key, transactionInformation.Id);
				helper.EnsureDocumentEtagMatchInTransaction(key, etag);

				state.Documents.Set(key, new DocuementWrapper
				{
					Document = doc.Document,
					LockByTransaction = transactionInformation.Id
				});
			}
			else
			{
				helper.EnsureDocumentIsNotCreatedInAnotherTransaction(key, transactionInformation.Id);
			}

			helper.EnsureTransactionExists(transactionInformation);
			var newEtag = generator.CreateSequentialUuid();

			state.DocumentsModifiedByTransations.Set(key, new DocumentsModifiedByTransation
			{
				Document = new JsonDocument
				{
					Key = key,
					Metadata = metadata,
					Etag = newEtag,
					DataAsJson = data,
					LastModified = SystemTime.UtcNow
				},
				LockByTransaction = transactionInformation.Id,
				DeleteDocument = false
			});

			return newEtag;
		}

		public bool DeleteDocumentInTransaction(TransactionInformation transactionInformation, string key, Guid? etag)
		{
			var doc = state.Documents.GetOrDefault(key);

			if (doc == null)
				return false;

			helper.EnsureNotLockedByTransaction(key, transactionInformation.Id);
			helper.EnsureDocumentEtagMatchInTransaction(key, etag);

			state.Documents.Set(key, new DocuementWrapper
			{
				Document = doc.Document,
				LockByTransaction = transactionInformation.Id
			});

			helper.EnsureTransactionExists(transactionInformation);

			var newEtag = generator.CreateSequentialUuid();

			state.DocumentsModifiedByTransations.Set(key, new DocumentsModifiedByTransation
			{
				Document = new JsonDocument
				{
					Key = key,
					Etag = newEtag
				},
				DeleteDocument = true,
				LockByTransaction = transactionInformation.Id
			});

			return true;
		}

		public void RollbackTransaction(Guid txId)
		{
			CompleteTransaction(txId, doc =>
			{
				var document = state.Documents.GetOrDefault(doc.Key);

				if (document != null)
				{
					helper.ResetTransactionOnDocument(doc.Key);
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

			helper.EnsureTransactionExists(transactionInformation);
			CompleteTransaction(fromTxId, doc =>
			{
				var document = state.Documents.GetOrDefault(doc.Key);

				if (document != null)
				{
					state.DocumentsModifiedByTransations.Set(document.Document.Key, new DocumentsModifiedByTransation
					{
						Document = document.Document,
						LockByTransaction = toTxId
					});
				}

				if (doc.Delete)
					DeleteDocumentInTransaction(transactionInformation, doc.Key, null);
				else
					AddDocumentInTransaction(doc.Key, null, doc.Data, doc.Metadata, transactionInformation);
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

		public IEnumerable<Guid> GetTransactionIds()
		{
			return state.Transactions.Select(pair => pair.Value.Key);
		}

		public bool TransactionExists(Guid txId)
		{
			return state.Transactions.Any(pair => pair.Value.Key == txId);
		}
	}
}
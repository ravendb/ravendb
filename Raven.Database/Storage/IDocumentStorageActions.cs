using System;
using System.Collections.Generic;
using Newtonsoft.Json.Linq;

namespace Raven.Database.Storage.StorageActions
{
	public interface IDocumentStorageActions 
	{
		Tuple<int, int> FirstAndLastDocumentIds();
		IEnumerable<Tuple<JsonDocument, int>> DocumentsById(int startId, int endId);
		IEnumerable<JsonDocument> GetDocumentsByReverseUpdateOrder(int start);
		IEnumerable<JsonDocument> GetDocumentsAfter(Guid etag);

		IEnumerable<string> DocumentKeys { get; }
		long GetDocumentsCount();
		JsonDocument DocumentByKey(string key, TransactionInformation transactionInformation);
		bool DeleteDocument(string key, Guid? etag, out JObject metadata);
		Guid AddDocument(string key, Guid? etag, JObject data, JObject metadata);
	}

	public interface ITransactionStorageActions
	{
		Guid AddDocumentInTransaction(string key, Guid? etag, JObject data, JObject metadata, TransactionInformation transactionInformation);
		void DeleteDocumentInTransaction(TransactionInformation transactionInformation, string key, Guid? etag);
		void RollbackTransaction(Guid txId);
		void ModifyTransactionId(Guid fromTxId, Guid toTxId, TimeSpan timeout);
		void CompleteTransaction(Guid txId, Action<DocumentInTransactionData> perDocumentModified);
		IEnumerable<Guid> GetTransactionIds();
	}
}

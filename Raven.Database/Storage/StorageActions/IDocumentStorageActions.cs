using System;
using System.Collections.Generic;
using Newtonsoft.Json.Linq;

namespace Raven.Database.Storage.StorageActions
{
	public interface IDocumentStorageActions
	{
		int GetDocumentsCount();
		IEnumerable<string> DocumentKeys { get; }
		JsonDocument DocumentByKey(string key, TransactionInformation transactionInformation);
		IEnumerable<JsonDocument> GetDocumentsByReverseUpdateOrder(int start);
		IEnumerable<JsonDocument> GetDocumentsAfter(Guid etag);
		IEnumerable<Tuple<JsonDocument, int>> DocumentsById(int startId, int endId);
		Guid AddDocument(string key, Guid? etag, JObject data, JObject metadata);
		Guid AddDocumentInTransaction(string key, Guid? etag, JObject data, JObject metadata, TransactionInformation transactionInformation);
		bool DeleteDocument(string key, Guid? etag, out JObject metadata);
		void DeleteDocumentInTransaction(TransactionInformation transactionInformation, string key, Guid? etag);
		Tuple<int, int> FirstAndLastDocumentIds();
	}
}
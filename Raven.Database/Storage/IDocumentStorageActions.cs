//-----------------------------------------------------------------------
// <copyright file="IDocumentStorageActions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using Newtonsoft.Json.Linq;
using Raven.Http;

namespace Raven.Database.Storage
{
	public interface IDocumentStorageActions 
	{
		Tuple<int, int> FirstAndLastDocumentIds();
		IEnumerable<Tuple<JsonDocument, int>> DocumentsById(int startId, int endId);
		IEnumerable<JsonDocument> GetDocumentsByReverseUpdateOrder(int start);
		IEnumerable<JsonDocument> GetDocumentsAfter(Guid etag);

		long GetDocumentsCount();
		JsonDocument DocumentByKey(string key, TransactionInformation transactionInformation);
		bool DeleteDocument(string key, Guid? etag, out JObject metadata);
		Guid AddDocument(string key, Guid? etag, JObject data, JObject metadata);
	}
}

//-----------------------------------------------------------------------
// <copyright file="IDocumentStorageActions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using Raven.Http;
using Raven.Json.Linq;

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
		JsonDocumentMetadata DocumentMetadataByKey(string key, TransactionInformation transactionInformation);

		bool DeleteDocument(string key, Guid? etag, out RavenJObject metadata);
		Guid AddDocument(string key, Guid? etag, RavenJObject data, RavenJObject metadata);
		IEnumerable<JsonDocument> GetDocumentsWithIdStartingWith(string idPrefix, int start);
	}
}

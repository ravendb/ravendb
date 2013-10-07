//-----------------------------------------------------------------------
// <copyright file="IIndexingStorageActions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Database.Data;
using Raven.Database.Indexing;

namespace Raven.Database.Storage
{
	public interface IIndexingStorageActions : IDisposable
	{
		IEnumerable<IndexStats> GetIndexesStats();

		IndexStats GetIndexStats(int index);
		void AddIndex(int id, bool createMapReduce);
	    void PrepareIndexForDeletion(int id);
		void DeleteIndex(int id, CancellationToken cancellationToken);

	    void SetIndexPriority(int id, IndexingPriority priority);


		IndexFailureInformation GetFailureRate(int id);

		void UpdateLastIndexed(int id, Etag etag, DateTime timestamp);
		void UpdateLastReduced(int id, Etag etag, DateTime timestamp);
		void TouchIndexEtag(int id);
		void UpdateIndexingStats(int id, IndexingWorkStats stats);
		void UpdateReduceStats(int id, IndexingWorkStats stats);

		void RemoveAllDocumentReferencesFrom(string key);
		void UpdateDocumentReferences(int id, string key, HashSet<string> references);
		IEnumerable<string> GetDocumentsReferencing(string key);
		int GetCountOfDocumentsReferencing(string key);
		IEnumerable<string> GetDocumentsReferencesFrom(string key);
	}
}

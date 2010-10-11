using System;
using System.Collections.Generic;
using Raven.Database.Data;

namespace Raven.Database.Storage.StorageActions
{
	public interface IIndexingStorageActions
	{
		void SetCurrentIndexStatsTo(string index);
		void FlushIndexStats();

		void IncrementIndexingAttempt();
		void IncrementSuccessIndexing();
		void IncrementIndexingFailure();
		void DecrementIndexingAttempt();

		IEnumerable<IndexStats> GetIndexesStats();
		void AddIndex(string name);
		void DeleteIndex(string name);


		IndexFailureInformation GetFailureRate(string index);
		void UpdateLastIndexed(string index, Guid etag, DateTime timestamp);
	}
}

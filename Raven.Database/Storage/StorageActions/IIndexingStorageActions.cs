using System.Collections.Generic;
using Raven.Database.Data;

namespace Raven.Database.Storage.StorageActions
{
	public interface IIndexingStorageActions
	{
		void SetCurrentIndexStatsTo(string index);
		void IncrementIndexingAttempt();
		void IncrementSuccessIndexing();
		void IncrementIndexingFailure();
		IEnumerable<IndexStats> GetIndexesStats();
		void AddIndex(string name);
		void DeleteIndex(string name);
		void DecrementIndexingAttempt();
		IndexFailureInformation GetFailureRate(string index);
	}
}
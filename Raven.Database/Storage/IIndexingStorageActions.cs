//-----------------------------------------------------------------------
// <copyright file="IIndexingStorageActions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Database.Data;

namespace Raven.Database.Storage
{
	public interface IIndexingStorageActions
	{
		void SetCurrentIndexStatsTo(string index);

		void IncrementIndexingAttempt();
		void IncrementSuccessIndexing();
		void IncrementIndexingFailure();
		void DecrementIndexingAttempt();

		void IncrementReduceIndexingAttempt();
		void IncrementReduceSuccessIndexing();
		void IncrementReduceIndexingFailure();
		void DecrementReduceIndexingAttempt();

		IEnumerable<IndexStats> GetIndexesStats();
		void AddIndex(string name);
		void DeleteIndex(string name);


		IndexFailureInformation GetFailureRate(string index);
		void UpdateLastIndexed(string index, Guid etag, DateTime timestamp);
	}
}

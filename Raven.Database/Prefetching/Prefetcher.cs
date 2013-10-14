// -----------------------------------------------------------------------
//  <copyright file="Prefetcher.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Database.Impl.Synchronization;
using Raven.Database.Indexing;

namespace Raven.Database.Prefetching
{
	public class Prefetcher
	{
		private readonly WorkContext workContext;
		private IDictionary<PrefetchingUser, PrefetchingBehavior> prefetchingBehaviors = new Dictionary<PrefetchingUser, PrefetchingBehavior>();

		public Prefetcher(WorkContext workContext)
		{
			this.workContext = workContext;
		}

		public PrefetchingBehavior GetPrefetchingBehavior(PrefetchingUser user, BaseBatchSizeAutoTuner autoTuner)
		{
			PrefetchingBehavior value;
			if (prefetchingBehaviors.TryGetValue(user, out value))
				return value;
			lock (this)
			{
				if (prefetchingBehaviors.TryGetValue(user, out value))
					return value;

				value = new PrefetchingBehavior(workContext, autoTuner ?? new IndependentBatchSizeAutoTuner(workContext));

				prefetchingBehaviors = new Dictionary<PrefetchingUser, PrefetchingBehavior>(prefetchingBehaviors)
				{
					{user, value}
				};
				return value;
			}
		}

		public void AfterDelete(string key, Etag deletedEtag)
		{
			foreach (var behavior in prefetchingBehaviors)
			{
				behavior.Value.AfterDelete(key, deletedEtag);
			}
		}

		public void AfterUpdate(string key, Etag etagBeforeUpdate)
		{
			foreach (var behavior in prefetchingBehaviors)
			{
				behavior.Value.AfterUpdate(key, etagBeforeUpdate);
			}
		}

		public int GetInMemoryIndexingQueueSize(PrefetchingUser user)
		{
			PrefetchingBehavior value;
			if (prefetchingBehaviors.TryGetValue(user, out value))
				return value.InMemoryIndexingQueueSize;
			return -1;
		}

		public void AfterStorageCommitBeforeWorkNotifications(PrefetchingUser user, JsonDocument[] documents)
		{
			PrefetchingBehavior value;
			if (prefetchingBehaviors.TryGetValue(user, out value) == false)
				return;
			value.AfterStorageCommitBeforeWorkNotifications(documents);
		}
	}
}
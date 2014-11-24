// -----------------------------------------------------------------------
//  <copyright file="Prefetcher.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Database.Indexing;

namespace Raven.Database.Prefetching
{
	using System.Linq;

	public class Prefetcher
	{
		private readonly WorkContext workContext;
		private List<PrefetchingBehavior> prefetchingBehaviors = new List<PrefetchingBehavior>();

		public Prefetcher(WorkContext workContext)
		{
			this.workContext = workContext;
		}

		public PrefetchingBehavior CreatePrefetchingBehavior(PrefetchingUser user, BaseBatchSizeAutoTuner autoTuner)
		{
			lock (this)
			{
				var newPrefetcher = new PrefetchingBehavior(user, workContext, autoTuner ?? new IndependentBatchSizeAutoTuner(workContext, user));

				prefetchingBehaviors = new List<PrefetchingBehavior>(prefetchingBehaviors)
				{
					newPrefetcher
				};

				return newPrefetcher;
			}
		}

		public void RemovePrefetchingBehavior(PrefetchingBehavior prefetchingBehavior)
		{
			lock (this)
			{
				prefetchingBehaviors = new List<PrefetchingBehavior>(prefetchingBehaviors.Except(new[]
				{
					prefetchingBehavior
				}));

				prefetchingBehavior.Dispose();
			}
		}

		public void AfterDelete(string key, Etag deletedEtag)
		{
			foreach (var behavior in prefetchingBehaviors)
			{
				behavior.AfterDelete(key, deletedEtag);
			}
		}

		public void AfterUpdate(string key, Etag etagBeforeUpdate)
		{
			foreach (var behavior in prefetchingBehaviors)
			{
				behavior.AfterUpdate(key, etagBeforeUpdate);
			}
		}

		public int[] GetInMemoryIndexingQueueSizes(PrefetchingUser user)
		{
			return prefetchingBehaviors.Where(x => x.PrefetchingUser == user).Select(value => value.InMemoryIndexingQueueSize).ToArray();
		}

		public void AfterStorageCommitBeforeWorkNotifications(PrefetchingUser user, JsonDocument[] documents)
		{
			foreach (var prefetcher in prefetchingBehaviors.Where(x => x.PrefetchingUser == user))
			{
				prefetcher.AfterStorageCommitBeforeWorkNotifications(documents);
			}
		}

		public void Dispose()
		{
			foreach (var prefetchingBehavior in prefetchingBehaviors)
			{
				prefetchingBehavior.Dispose();
			}
		}
	}
}
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
    using System;
    using System.Linq;

	public class Prefetcher
	{
		private readonly WorkContext workContext;
		private List<WeakReference> prefetchingBehaviors = new List<WeakReference>();

		public Prefetcher(WorkContext workContext)
		{
			this.workContext = workContext;
		}

		public PrefetchingBehavior CreatePrefetchingBehavior(PrefetchingUser user, BaseBatchSizeAutoTuner autoTuner)
		{
			lock (this)
			{
				var newPrefetcher = new PrefetchingBehavior(user, workContext, autoTuner ?? new IndependentBatchSizeAutoTuner(workContext));

				prefetchingBehaviors = new List<WeakReference>(prefetchingBehaviors.Where(x=>x.IsAlive))
				{
					new WeakReference(newPrefetcher)
				};

				return newPrefetcher;
			}
		}

		public void AfterDelete(string key, Etag deletedEtag)
		{
			foreach (var weakRef in prefetchingBehaviors)
			{
                var behavior = (PrefetchingBehavior)weakRef.Target;
                if (behavior == null)
                    continue;
                behavior.AfterDelete(key, deletedEtag);
			}
		}

		public void AfterUpdate(string key, Etag etagBeforeUpdate)
		{
			foreach (var weakRef in prefetchingBehaviors)
			{
                var behavior = (PrefetchingBehavior)weakRef.Target;
                if (behavior == null)
                    continue;
                behavior.AfterUpdate(key, etagBeforeUpdate);
			}
		}

		public int GetInMemoryIndexingQueueSize(PrefetchingUser user)
		{
            var value = prefetchingBehaviors
                .Select(x => x.Target)
                .OfType<PrefetchingBehavior>()
                .FirstOrDefault(x =>
                {
                    return x.PrefetchingUser == user;
                });
			if (value != null)
				return value.InMemoryIndexingQueueSize;
			return -1;
		}

		public void AfterStorageCommitBeforeWorkNotifications(PrefetchingUser user, JsonDocument[] documents)
		{
			foreach (var prefetcher in prefetchingBehaviors
                .Select(x => x.Target)
                .OfType<PrefetchingBehavior>()
                .Where(x =>
                {
                    return x.PrefetchingUser == user;
                }))
			{
				prefetcher.AfterStorageCommitBeforeWorkNotifications(documents);
			}
		}
	}
}
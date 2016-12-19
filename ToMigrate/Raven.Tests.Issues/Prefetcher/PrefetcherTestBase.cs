// -----------------------------------------------------------------------
//  <copyright file="PrefetcherBase.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;

using Raven.Abstractions.Data;
using Raven.Abstractions.MEF;
using Raven.Database.Config;
using Raven.Database.Indexing;
using Raven.Database.Plugins;
using Raven.Database.Prefetching;
using Raven.Database.Util;
using Raven.Json.Linq;
using Raven.Storage.Voron;
using Raven.Tests.Common.Dto;

namespace Raven.Tests.Issues.Prefetcher
{
    public abstract class PrefetcherTestBase : IDisposable
    {
        private readonly List<PrefetcherWithContext> createdPrefetchers = new List<PrefetcherWithContext>();

        protected PrefetcherWithContext CreatePrefetcher(
            Action<InMemoryRavenConfiguration> modifyConfiguration = null, 
            Action<WorkContext> modifyWorkContext = null,
            HashSet<string> entityNames = null)
        {
            var configuration = new InMemoryRavenConfiguration
            {
                RunInMemory = true
            };

            configuration.Initialize();

            if (modifyConfiguration != null)
                modifyConfiguration(configuration);

            var transactionalStorage = new TransactionalStorage(configuration, () => { }, () => { }, () => { }, () => { });
            transactionalStorage.Initialize(new SequentialUuidGenerator { EtagBase = 0 }, new OrderedPartCollection<AbstractDocumentCodec>());
            
            var workContext = new WorkContext
            {
                Configuration = configuration,
                TransactionalStorage = transactionalStorage
            };

            if (modifyWorkContext != null)
                modifyWorkContext(workContext);

            var autoTuner = new IndexBatchSizeAutoTuner(workContext);

            var prefetchingBehavior = new PrefetchingBehavior(PrefetchingUser.Indexer, workContext, autoTuner, string.Empty, entityNames);

            var prefetcherWithContext = new PrefetcherWithContext
                                        {
                                            AutoTuner = autoTuner,
                                            Configuration = configuration,
                                            PrefetchingBehavior = prefetchingBehavior,
                                            TransactionalStorage = transactionalStorage,
                                            WorkContext = workContext
                                        };

            createdPrefetchers.Add(prefetcherWithContext);

            return prefetcherWithContext;
        }

        protected List<string> AddDocumentsToTransactionalStorage(TransactionalStorage transactionalStorage, int numberOfDocuments)
        {
            var results = new List<string>();

            transactionalStorage.Batch(accessor =>
            {
                for (var i = 0; i < numberOfDocuments; i++)
                {
                    var key = "keys/" + i;
                    var data = RavenJObject.FromObject(new Person { AddressId = key, Id = key, Name = "Name" + i });
                    accessor.Documents.AddDocument(key, null, data, new RavenJObject
                    {
                        {"Raven-Entity-Name", "Keys"}
                    });

                    results.Add(key);
                }
            });

            return results;
        }

        public void Dispose()
        {
            foreach (var prefetcherWithContext in createdPrefetchers)
            {
                prefetcherWithContext.Dispose();
            }
        }

        protected class PrefetcherWithContext : IDisposable
        {
            public PrefetchingBehavior PrefetchingBehavior { get; set; }

            public InMemoryRavenConfiguration Configuration { get; set; }

            public WorkContext WorkContext { get; set; }

            public IndexBatchSizeAutoTuner AutoTuner { get; set; }

            public TransactionalStorage TransactionalStorage { get; set; }

            public void Dispose()
            {
                WorkContext.StopWorkRude(); //make sure cancellation token is at "cancel" state
                WorkContext.Dispose();

                PrefetchingBehavior.Dispose();
                TransactionalStorage.Dispose();
            }
        }
    }
}

using Raven.Abstractions.Data;
using Raven.Tests.Issues.Prefetcher;
using System;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_5390 : PrefetcherTestBase
    {
        [Fact]
        public void Frequent_updates_of_document_should_not_cause_deadlock_in_prefetcher()
        {
            // this test does not guarantee to fail on very run however 9/10 runs is failed

            var prefetcher = CreatePrefetcher();
            var futureBatchCreated = false;
            var task = Task.Factory.StartNew(() =>
            {
                for (int i = 0; i < 1000; i++)
                {
                    AddDocumentsToTransactionalStorage(prefetcher.TransactionalStorage, 1);

                    if (prefetcher.PrefetchingBehavior.InMemoryFutureIndexBatchesSize > 0)
                    {
                        futureBatchCreated = true;
                        break;
                    }
                }
            });

            Assert.True(SpinWait.SpinUntil(() =>
            {
                prefetcher.PrefetchingBehavior.GetDocumentsBatchFrom(Etag.Empty, 1); // get docs to ensure a future batch will be added

                return prefetcher.PrefetchingBehavior.InMemoryFutureIndexBatchesSize > 0 || futureBatchCreated;
            }, TimeSpan.FromSeconds(10)));
            
            task.Wait();

            AddDocumentsToTransactionalStorage(prefetcher.TransactionalStorage, 1);

            var docs = prefetcher.PrefetchingBehavior.GetDocumentsBatchFrom(Etag.Empty, 1);

            Assert.Equal(1, docs.Count);
        }
    }
}

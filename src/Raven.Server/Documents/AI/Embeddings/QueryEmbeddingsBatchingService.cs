using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Documents.ETL.Providers.AI;
using Sparrow.Server.Logging;

namespace Raven.Server.Documents.AI.Embeddings
{
    public sealed class QueryEmbeddingsBatchingService(AiIntegrationsController aiIntegrations) : IDisposable
    {
        private readonly SemaphoreSlim _globalConcurrencyLimiter = new(aiIntegrations.Database.Configuration.Ai.QueryEmbeddingsMaxConcurrentBatches);
        private readonly RavenLogger _logger = aiIntegrations.Database.Loggers.GetLogger<QueryEmbeddingsBatchingService>();

        private readonly ConcurrentDictionary<AiConnectionStringIdentifier, QueryEmbeddingsBatchingWorker> _batchWorkers = new();

        public ValueTask<ReadOnlyMemory<float>[]> GetEmbeddingAsync(AiConnectionStringIdentifier connectionStringId, IList<string> values, CancellationToken cancellationToken = default)
        {
            var batchWorker = _batchWorkers.GetOrAdd(connectionStringId, aiConnectionStringIdentifier =>
            {
                if (aiIntegrations.TryGetServiceByConnectionString(connectionStringId, out var service) == false)
                    throw new ArgumentException($"Couldn't find embedding generation service for connection string '{connectionStringId.Value}'");

                var worker = new QueryEmbeddingsBatchingWorker(aiIntegrations.Database.Name, aiIntegrations.Database.Configuration.Ai, service, aiConnectionStringIdentifier, _globalConcurrencyLimiter, _logger, aiIntegrations.Database.DatabaseShutdown);

                worker.Start();
                return worker;
            });

            return new ValueTask<ReadOnlyMemory<float>[]>(batchWorker.EnqueueRequestAsync(values, cancellationToken));
        }

        public void Dispose()
        {
            foreach (var worker in _batchWorkers.Values)
                worker.Dispose();

            _globalConcurrencyLimiter.Dispose();
        }

        internal TestingStuff ForTestingPurposes;

        internal TestingStuff ForTestingPurposesOnly()
        {
            if (ForTestingPurposes != null)
                return ForTestingPurposes;

            return ForTestingPurposes = new TestingStuff(_batchWorkers);
        }

        internal sealed class TestingStuff
        {
            private readonly ConcurrentDictionary<AiConnectionStringIdentifier, QueryEmbeddingsBatchingWorker> _batchWorkers;

            public TestingStuff(ConcurrentDictionary<AiConnectionStringIdentifier, QueryEmbeddingsBatchingWorker> batchWorkers)
            {
                _batchWorkers = batchWorkers;
            }

            public QueryEmbeddingsBatchingWorker GetBatchWorker(AiConnectionStringIdentifier connectionStringId)
            {
                return _batchWorkers[connectionStringId];
            }
        }
    }
}

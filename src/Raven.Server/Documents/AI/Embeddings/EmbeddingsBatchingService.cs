using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Documents.ETL.Providers.AI;
using Sparrow.Server.Logging;

namespace Raven.Server.Documents.AI.Embeddings
{
    public sealed class EmbeddingsBatchingService(DocumentDatabase database, AiIntegrationsController aiIntegrations) : IDisposable
    {
        private readonly SemaphoreSlim _globalConcurrencyLimiter = new(database.Configuration.Ai.MaxConcurrentBatches);
        private readonly RavenLogger _logger = database.Loggers.GetLogger<EmbeddingsBatchingService>();

        private readonly ConcurrentDictionary<AiConnectionStringIdentifier, EmbeddingsBatchingWorker> _batchWorkers = new();

        public ValueTask<ReadOnlyMemory<float>> GetEmbeddingAsync(AiConnectionStringIdentifier connectionStringId, string value, CancellationToken cancellationToken = default)
        {
            if (aiIntegrations.TryGetServiceByConnectionString(connectionStringId, out var service) == false)
                throw new ArgumentException($"Couldn't find Embeddings Generation task for connection string '{connectionStringId.Value}'");

            var batchWorker = _batchWorkers.GetOrAdd(connectionStringId, aiConnectionStringIdentifier =>
            {
                var worker = new EmbeddingsBatchingWorker(database.Name, database.Configuration.Ai, service, aiConnectionStringIdentifier, _globalConcurrencyLimiter, _logger, database.DatabaseShutdown);

                worker.Start();
                return worker;
            });

            return new ValueTask<ReadOnlyMemory<float>>(batchWorker.EnqueueRequestAsync(value, cancellationToken));
        }

        public void Dispose()
        {
            foreach (var worker in _batchWorkers.Values)
                worker.Dispose();

            _globalConcurrencyLimiter.Dispose();
        }
    }
}

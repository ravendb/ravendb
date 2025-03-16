using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Util;
using Raven.Server.Documents.ETL.Providers.AI;
using Sparrow.Server.Logging;

namespace Raven.Server.Documents.AI.Embeddings
{
    public sealed class QueryEmbeddingsBatchingService(AiIntegrationsController aiIntegrations) : IDisposable
    {
        private readonly RavenLogger _logger = aiIntegrations.Database.Loggers.GetLogger<QueryEmbeddingsBatchingService>();

        private readonly ConcurrentDictionary<AiConnectionStringIdentifier, QueryEmbeddingsBatchingWorker> _batchWorkers = new();
        // Flag for preventing the addition of new workers during disposal
        private volatile bool _isDisposing;

        public ValueTask<ReadOnlyMemory<float>[]> GetEmbeddingAsync(AiConnectionStringIdentifier connectionStringId, IList<string> values, CancellationToken cancellationToken = default)
        {
            if (_isDisposing)
            {
                var tcs = new TaskCompletionSource<ReadOnlyMemory<float>[]>(TaskCreationOptions.RunContinuationsAsynchronously);
                tcs.SetException(new ObjectDisposedException($"{nameof(QueryEmbeddingsBatchingService)} is shutting down"));
                return new ValueTask<ReadOnlyMemory<float>[]>(tcs.Task);
            }

            var batchWorker = _batchWorkers.GetOrAdd(connectionStringId, CreateBatchWorker);

            return new ValueTask<ReadOnlyMemory<float>[]>(batchWorker.EnqueueRequestAsync(values, cancellationToken));
        }

        private QueryEmbeddingsBatchingWorker CreateBatchWorker(AiConnectionStringIdentifier connectionStringId)
        {
            if (aiIntegrations.TryGetServiceByConnectionString(connectionStringId, out var service) == false)
                throw new ArgumentException($"Couldn't find embedding generation service for connection string '{connectionStringId.Value}'");

            var worker = new QueryEmbeddingsBatchingWorker(aiIntegrations.Database.Name, aiIntegrations.Database.Configuration.Ai, service, connectionStringId, _logger, aiIntegrations.Database.DatabaseShutdown);

            worker.Start();
            return worker;
        }

        public void RecreateWorker(AiConnectionString newConnectionString)
        {
            var connectionStringId = new AiConnectionStringIdentifier(newConnectionString.Identifier);
            if (_batchWorkers.TryGetValue(connectionStringId, out var worker) == false)
                return;

            RemoveWorker(connectionStringId);
            _batchWorkers.TryAdd(connectionStringId, CreateBatchWorker(connectionStringId));
        }

        public void RemoveWorker(AiConnectionStringIdentifier connectionStringsToRemove)
        {
            ArgumentNullException.ThrowIfNull(connectionStringsToRemove);

            if (_batchWorkers.TryRemove(connectionStringsToRemove, out var worker) == false)
                return;

            try
            {
                worker.Dispose();

                if (_logger.IsInfoEnabled)
                    _logger.Info($"Removed and disposed batch worker for connection string '{connectionStringsToRemove.Value}'");
            }
            catch (Exception ex)
            {
                if (_logger.IsErrorEnabled)
                    _logger.Error($"Error disposing batch worker for connection string '{connectionStringsToRemove.Value}': {ex}");
            }
        }

        public Task DisposeAsync()
        {
            _isDisposing = true;

            foreach (var worker in _batchWorkers.Values)
            {
                try
                {
                    worker.Dispose();
                }
                catch (Exception ex)
                {
                    if (_logger.IsErrorEnabled)
                        _logger.Error($"Error disposing worker: {ex.Message}", ex);
                }
            }

            _batchWorkers.Clear();

            return Task.CompletedTask;
        }

        public void Dispose() => AsyncHelpers.RunSync(DisposeAsync);

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

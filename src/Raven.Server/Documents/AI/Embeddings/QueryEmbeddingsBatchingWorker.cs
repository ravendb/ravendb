using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.SemanticKernel;
using Microsoft.SemanticKernel.Embeddings;
using Raven.Server.Background;
using Raven.Server.Config;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.ETL.Providers.AI;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings;
using Sparrow.Server.Logging;

namespace Raven.Server.Documents.AI.Embeddings
{
#pragma warning disable SKEXP0001

    public class QueryEmbeddingsBatchingWorker : BackgroundWorkBase
    {
        private readonly string _databaseName;
        private readonly AiConfiguration _configuration;
        private readonly ITextEmbeddingGenerationService _service;
        private readonly AiConnectionStringIdentifier _connectionStringIdentifier;

        private readonly ConcurrentQueue<QueryEmbeddingsRequest> _requestQueue = new();
        private readonly AutoResetEvent _workAvailable = new(false);

        private readonly Task[] _workerTasks;
        
        private readonly SemaphoreSlim _queueReaderLock = new(1, 1);

        public QueryEmbeddingsBatchingWorker(string databaseName,
            AiConfiguration configuration,
            ITextEmbeddingGenerationService service,
            AiConnectionStringIdentifier connectionStringIdentifier,
            RavenLogger logger,
            CancellationToken shutdown) : base(nameof(QueryEmbeddingsBatchingWorker), logger, shutdown)
        {
            _databaseName = databaseName;
            _configuration = configuration;
            _service = service;
            _connectionStringIdentifier = connectionStringIdentifier;

            int workerCount = configuration.QueryEmbeddingsMaxConcurrentBatches;
            _workerTasks = new Task[workerCount];

            if (logger.IsInfoEnabled)
                logger.Info($"Initializing {nameof(QueryEmbeddingsBatchingWorker)} for connection '{_connectionStringIdentifier.Value}' in database '{databaseName}' with {workerCount} workers");
        }

        public Task<ReadOnlyMemory<float>[]> EnqueueRequestAsync(IList<string> values, CancellationToken cancellationToken)
        {
            CancellationToken.ThrowIfCancellationRequested();
            
            var request = new QueryEmbeddingsRequest(values, cancellationToken);

            _requestQueue.Enqueue(request);

            _workAvailable.Set();

            return request.TaskCompletionSource.Task;
        }

        private async Task WorkerLoopAsync()
        {
            while (CancellationToken.IsCancellationRequested == false)
            {
                try
                {
                    WaitHandle.WaitAny([_workAvailable, CancellationToken.WaitHandle]);

                    CancellationToken.ThrowIfCancellationRequested();

                    if (await _queueReaderLock.WaitAsync(0, CancellationToken))
                    {
                        List<QueryEmbeddingsRequest> requests;
                        var totalNumberOfValues = 0;

                        var mightBeMore = false;

                        try
                        {
                            requests = new List<QueryEmbeddingsRequest>();

                            while (_requestQueue.TryDequeue(out var request)) // todo: take into account model's token limit
                            {
                                if (request.TaskCompletionSource.Task.IsCanceled)
                                    continue;

                                requests.Add(request);

                                totalNumberOfValues += request.Values.Count;

                                if (requests.Count >= _configuration.QueryEmbeddingsMaxBatchSize)
                                {
                                    mightBeMore = true;
                                    break;
                                }
                            }
                        }
                        finally
                        {
                            _queueReaderLock.Release();
                        }

                        if (mightBeMore)
                            _workAvailable.Set();

                        if (requests.Count > 0)
                            await ProcessBatchAsync(requests, totalNumberOfValues);
                    }
                }
                catch (OperationCanceledException)
                {
                    while (_requestQueue.TryDequeue(out var request))
                        request.TaskCompletionSource.TrySetCanceled();
                    
                    break;
                }
                catch (Exception ex)
                {
                    if (Logger.IsErrorEnabled)
                        Logger.Error($"Error in query embeddings batching worker for connection string '{_connectionStringIdentifier.Value}' in database '{_databaseName}'", ex);
                }
            }
        }

        private async Task ProcessBatchAsync(List<QueryEmbeddingsRequest> requests, int totalNumberOfValues)
        {
            Stopwatch stopwatch = null;
            int count = 0;

            try
            {
                if (Logger.IsDebugEnabled)
                    stopwatch = Stopwatch.StartNew();

                await GenerateEmbeddingsForBatchAsync(requests, totalNumberOfValues);

                ForTestingPurposes?.AfterBatchProcessed?.Invoke();
            }
            catch (OperationCanceledException)
            {
                PropagateCancellationToCallers(requests);
            }
            catch (Exception ex)
            {
                PropagateExceptionToCallers(requests, ex);

                if (Logger.IsErrorEnabled)
                    Logger.Error($"Error in batch processing for connection string '{_connectionStringIdentifier.Value}' in database '{_databaseName}'", ex);
            }
            finally
            {
                if (Logger.IsDebugEnabled && stopwatch != null)
                    Logger.Debug($"Embeddings generation batch processing completed for connection '{_connectionStringIdentifier.Value}' in {stopwatch.ElapsedMilliseconds}ms, processed {count} requests");
            }
        }

        private async Task GenerateEmbeddingsForBatchAsync(List<QueryEmbeddingsRequest> requests, int totalValueCount)
        {
            var count = requests.Count;

            var allTextValues = new string[totalValueCount];

            var valueRanges = new (int StartIndex, int Count)[count];

            var currentIndex = 0;
            
            for (int i = 0; i < count; i++)
            {
                var values = requests[i].Values;
                valueRanges[i] = (currentIndex, values.Count);

                foreach (var value in values)
                    allTextValues[currentIndex++] = value;
            }

            if (Logger.IsDebugEnabled)
                Logger.Debug($"Processing batch of {totalValueCount} values from {count} requests to generate embeddings for connection '{_connectionStringIdentifier.Value}'");

            IList<ReadOnlyMemory<float>> allEmbeddings;
            try
            {
                allEmbeddings = await AiHelper.GenerateEmbeddingsAsync(_service, allTextValues, CancellationToken);
            }
            catch (HttpOperationException httpOperationException) when (httpOperationException.StatusCode == System.Net.HttpStatusCode.TooManyRequests)
            {
                throw new EmbeddingGenerationException(
                    $"Failed to generate embeddings due to rate limits. Consider decreasing the number of elements processed in a single batch " +
                    $"('{RavenConfiguration.GetKey(x => x.Ai.QueryEmbeddingsMaxBatchSize)}') or increasing the " +
                    $"limits on your model deployment.", httpOperationException);
            }

            if (allEmbeddings.Count != totalValueCount)
                throw new InvalidOperationException($"Failed to generate embeddings: expected {totalValueCount} embeddings, but got {allEmbeddings.Count}");

            // Distribute results back to the requests - this needs to be done regardless of cancellation
            // If the request was canceled, we still need to create and return the result to cache the embeddings

            for (int i = 0; i < count; i++)
            {
                var request = requests[i];
                (int startIndex, int itemsCount) = valueRanges[i];

                var requestEmbeddings = new ReadOnlyMemory<float>[itemsCount];

                for (int j = 0; j < itemsCount; j++)
                    requestEmbeddings[j] = allEmbeddings[startIndex + j];

                // Return the list of embeddings to the caller - even if the request was canceled
                // If request was already canceled, TrySetResult will silently fail, but we don't care
                // because we just want to ensure embeddings are processed for caching
                request.TaskCompletionSource.TrySetResult(requestEmbeddings);
            }
        }

        private static void PropagateExceptionToCallers(List<QueryEmbeddingsRequest> requests, Exception ex)
        {
            if (requests == null || requests.Count == 0)
                return;

            foreach (var request in requests)
            {
                request.TaskCompletionSource.TrySetException(ex);
            }
        }

        private static void PropagateCancellationToCallers(List<QueryEmbeddingsRequest> requests)
        {
            if (requests == null || requests.Count == 0)
                return;

            foreach (var request in requests)
            {
                request.TaskCompletionSource.TrySetCanceled();
            }
        }

        protected override async Task DoWork()
        {
            for (int i = 0; i < _workerTasks.Length; i++)
                _workerTasks[i] = Task.Run(WorkerLoopAsync, CancellationToken);

            try
            {
                await Task.WhenAll(_workerTasks);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                if (Logger.IsErrorEnabled)
                    Logger.Error("Error waiting for worker tasks to complete", ex);

                throw;
            }
        }

        public override void Dispose()
        {
            base.Dispose();
            
            _workAvailable.Dispose();
        }

        internal TestingStuff ForTestingPurposes;

        internal TestingStuff ForTestingPurposesOnly()
        {
            if (ForTestingPurposes != null)
                return ForTestingPurposes;

            return ForTestingPurposes = new TestingStuff();
        }

        internal sealed class TestingStuff
        {
            internal Action AfterBatchProcessed;
        }
    }
#pragma warning restore SKEXP0001
}

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.SemanticKernel;
using Microsoft.SemanticKernel.Embeddings;
using Raven.Client.Documents.Operations.AI;
using Raven.Server.Background;
using Raven.Server.Config;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings;
using Sparrow.Server.Logging;

namespace Raven.Server.Documents.AI.Embeddings
{
    public class QueryEmbeddingsBatchingWorker(string databaseName,
        AiConfiguration configuration,
#pragma warning disable SKEXP0001
        (AiConnectionString ConnectionString, ITextEmbeddingGenerationService Instance) service,
#pragma warning restore SKEXP0001
        SemaphoreSlim concurrencyLimiter,
        RavenLogger logger,
        CancellationToken shutdown)
        : BackgroundWorkBase(databaseName, logger, shutdown)
    {
        private readonly string _databaseName = databaseName;

        private readonly ConcurrentQueue<QueryEmbeddingsBatchRequest> _requestQueue = new();
        private readonly Stopwatch _batchTimer = new();

        // Flag that indicates the service is being disposed
        private volatile bool _workerShuttingDown;
        private int _activeOperations;

        public Task<ReadOnlyMemory<float>[]> EnqueueRequestAsync(IList<string> values, CancellationToken cancellationToken)
        {
            var request = new QueryEmbeddingsBatchRequest(values, callerToken: cancellationToken, workerToken: CancellationToken);

            if (_workerShuttingDown)
                return request.CancelWithShutdownMessage();

            bool wasEmpty = _requestQueue.IsEmpty;
            _requestQueue.Enqueue(request);

            // Start the batch timer if this is the first request
            if (wasEmpty)
                _batchTimer.Restart();

            return request.TaskCompletionSource.Task;
        }

        protected override async Task DoWork()
        {
            if (_workerShuttingDown)
                return;

            try
            {
                // Check if we have requests to process
                if (_requestQueue.IsEmpty)
                {
                    // No requests, wait for a bit before checking again
                    await WaitOrThrowOperationCanceled(TimeSpan.FromMilliseconds(10));
                    return;
                }

                if (_batchTimer.IsRunning == false)
                    _batchTimer.Restart();

                // Nagle's algorithm:
                // 1. If we have a full batch, process immediately
                // 2. If the timeout has elapsed, process immediately
                // 3. Otherwise, wait for more requests longer
                bool shouldProcessNow = _requestQueue.Count >= configuration.QueryEmbeddingsMaxBatchSize ||
                                        _batchTimer.ElapsedMilliseconds >= configuration.QueryEmbeddingsBatchTimeout;

                if (shouldProcessNow == false)
                {
                    // Wait a bit before checking again
                    await WaitOrThrowOperationCanceled(TimeSpan.FromMilliseconds(Math.Min(10, configuration.QueryEmbeddingsBatchTimeout / 5)));
                    return;
                }

                // Reset timer
                _batchTimer.Reset();

                // Process the batch
                await concurrencyLimiter.WaitAsync(CancellationToken);

                try
                {
                    await ProcessBatchAsync();
                }
                finally
                {
                    concurrencyLimiter.Release();

                    // Restart timer if we have pending requests
                    if (_requestQueue.IsEmpty == false)
                        _batchTimer.Restart();
                }
            }
            catch (OperationCanceledException)
            {
                // Expected during shutdown, just rethrow
                throw;
            }
            catch (Exception ex)
            {
                if (Logger.IsErrorEnabled)
                    Logger.Error($"Error in QueryEmbeddingsBatchingWorker for connection string '{service.ConnectionString.Name}' in database '{_databaseName}'", ex);

                // Wait a bit before retrying
                await WaitOrThrowOperationCanceled(TimeSpan.FromSeconds(1));
            }
        }

        private async Task ProcessBatchAsync()
        {
            Interlocked.Increment(ref _activeOperations);

            try
            {
                // Check if shutting down
                if (_workerShuttingDown)
                    return;

                // Collect requests for this batch (up to QueryEmbeddingsMaxBatchSize)
                var requestsArray = new QueryEmbeddingsBatchRequest[configuration.QueryEmbeddingsMaxBatchSize];
                int count = 0;

                while (count < configuration.QueryEmbeddingsMaxBatchSize &&
                       _requestQueue.TryDequeue(out var request))
                {
                    if (request.TaskCompletionSource.Task.IsCanceled)
                        continue;

                    requestsArray[count++] = request;
                }

                if (count == 0)
                    return;

                var stopwatch = Stopwatch.StartNew();

                try
                {
                    // Re-check if shutting down before doing expensive work
                    if (_workerShuttingDown)
                    {
                        CancelRequestsWithShutdownMessage(requestsArray, count);
                        return;
                    }

                    await FlushBatchAsync(requestsArray, count);
                    ForTestingPurposes?.AfterBatchFlushed?.Invoke();
                }
                finally
                {
                    stopwatch.Stop();

                    if (Logger.IsDebugEnabled)
                        Logger.Debug($"Batch processing completed for connection '{service.ConnectionString.Identifier}' in {stopwatch.ElapsedMilliseconds}ms, processed {count} requests");
                }
            }
            finally
            {
                Interlocked.Decrement(ref _activeOperations);
            }
        }

        private async Task FlushBatchAsync(QueryEmbeddingsBatchRequest[] requestsArray, int count)
        {
            if (_workerShuttingDown)
            {
                CancelRequestsWithShutdownMessage(requestsArray, count);
                return;
            }

            // First calculate total number of values across all requests
            int totalValueCount = 0;
            for (int i = 0; i < count; i++)
                totalValueCount += requestsArray[i].Values.Count;
            
            var allTextValues = new string[totalValueCount];
            
            // Create tracking structure for the range of values for each request
            var valueRanges = new (int StartIndex, int Count)[count];

            // Fill the array with all values and remember the ranges
            int currentIndex = 0;
            for (int i = 0; i < count; i++)
            {
                var values = requestsArray[i].Values;
                valueRanges[i] = (currentIndex, values.Count);

                foreach (var value in values)
                    allTextValues[currentIndex++] = value;
            }
                
            // Execute with retry logic
            for (int attempt = 0; attempt <= configuration.QueryEmbeddingsBatchMaxRetries; attempt++)
            {
                try
                {
                    if (attempt > 0)
                    {
                        if (Logger.IsWarnEnabled)
                            Logger.Warn($"Retrying batch for connection '{service.ConnectionString.Name}', attempt {attempt}/{configuration.QueryEmbeddingsBatchMaxRetries}");

                        // Exponential backoff
                        var delay = configuration.QueryEmbeddingsBatchRetryDelay.AsTimeSpan * Math.Pow(2, attempt - 1);
                        await WaitOrThrowOperationCanceled(delay);
                    }

                    if (Logger.IsDebugEnabled)
                        Logger.Debug($"Processing batch of {totalValueCount} values from {count} requests for connection '{service.ConnectionString.Name}'");

                    // Final check before calling service
                    if (_workerShuttingDown)
                        throw new OperationCanceledException(QueryEmbeddingsBatchingService.ShutdownMessage);

                    IList<ReadOnlyMemory<float>> allEmbeddings;

                    try
                    {
#pragma warning disable SKEXP0001
                        allEmbeddings = await AiHelper.GenerateEmbeddingsAsync(service.Instance, allTextValues);
#pragma warning restore SKEXP0001
                    }
                    catch (HttpOperationException httpOperationException) when (httpOperationException.StatusCode == System.Net.HttpStatusCode.TooManyRequests)
                    {
                        // Handle rate limit errors
                        throw new EmbeddingGenerationException(
                            $"Failed to generate embeddings due to rate limits. The process will increase the delay between calls to the model. " +
                            $"However, decreasing the number of elements processed in a single batch " +
                            $"('{RavenConfiguration.GetKey(x => x.Ai.QueryEmbeddingsMaxBatchSize)}') may help, or you can increase the " +
                            $"limits on your model deployment.", httpOperationException);
                    }

                    // Verify we got the expected number of embeddings
                    if (allEmbeddings.Count != totalValueCount)
                        throw new InvalidOperationException($"Failed to generate embeddings: expected {totalValueCount} embeddings, but got {allEmbeddings.Count}");

                    // Distribute results back to the requests
                    for (int i = 0; i < count; i++)
                    {
                        var request = requestsArray[i];
                        (int startIndex, int itemsCount) = valueRanges[i];

                        ReadOnlyMemory<float>[] requestEmbeddings = new ReadOnlyMemory<float>[itemsCount];
                            
                        // Fill the temp array with embeddings for this request
                        for (int j = 0; j < itemsCount; j++)
                            requestEmbeddings[j] = allEmbeddings[startIndex + j];

                        // Return the list of embeddings to the caller
                        request.TaskCompletionSource.TrySetResult(requestEmbeddings);
                    }

                    // Successfully processed batch, exit retry loop
                    break;
                }
                catch (Exception ex) when (attempt < configuration.QueryEmbeddingsBatchMaxRetries && IsNonRetriableException(ex) == false)
                {
                    if (Logger.IsWarnEnabled)
                        Logger.Warn($"Error processing batch for connection '{service.ConnectionString.Name}', retrying {attempt + 1}/{configuration.QueryEmbeddingsBatchMaxRetries}", ex);

                    // Continue to next retry iteration
                }
                catch (Exception ex)
                {
                    if (Logger.IsErrorEnabled)
                        Logger.Error($"Final error processing batch for connection '{service.ConnectionString.Name}' after {attempt} retries", ex);

                    for (int i = 0; i < count; i++)
                        requestsArray[i].TaskCompletionSource.TrySetException(ex);
                }
            }
        }

        private static bool IsNonRetriableException(Exception ex) =>
            ex switch
            {
                // Check for specific exceptions that are not retriable
                ArgumentException or InvalidOperationException or UnauthorizedAccessException or OperationCanceledException => true,

                // Client errors (4xx) are generally not worth retrying, except for a few specific codes
                HttpOperationException { StatusCode: not null } httpEx when (int)httpEx.StatusCode >= 400 && (int)httpEx.StatusCode < 500 =>
                    httpEx.StatusCode switch
                    {
                        System.Net.HttpStatusCode.RequestTimeout => false, // 408
                        System.Net.HttpStatusCode.TooManyRequests => false, // 429
                        System.Net.HttpStatusCode.GatewayTimeout => false, // 504
                        _ => true // Other 4xx errors are non-retriable
                    },

                // Server errors (5xx) are generally worth retrying
                HttpOperationException { StatusCode: not null } httpEx when (int)httpEx.StatusCode >= 500 && (int)httpEx.StatusCode < 600 => false,

                // For EmbeddingGenerationException, check the inner exception
                EmbeddingGenerationException { InnerException: not null } embEx => IsNonRetriableException(embEx.InnerException),

                // For all other exceptions, assume they are retriable
                _ => false
            };

        public AiSettingsCompareDifferences Compare(AiConnectionString connectionString) => service.ConnectionString.Compare(connectionString);

        private static void CancelRequestsWithShutdownMessage(QueryEmbeddingsBatchRequest[] requests, int count)
        {
            for (int i = 0; i < count; i++)
                requests[i].CancelWithShutdownMessage();
        }

        protected override void InitializeWork()
        {
            if (Logger.IsInfoEnabled)
                Logger.Info($"Initializing {nameof(QueryEmbeddingsBatchingWorker)} for connection '{service.ConnectionString.Name}' in database '{_databaseName}'");

            _batchTimer.Reset();
        }

        public async Task PrepareForServiceDisposalAsync()
        {
            _workerShuttingDown = true;

            // Cancel all queued items
            while (_requestQueue.TryDequeue(out var request))
                await request.CancelWithShutdownMessage();

            var timeout = TimeSpan.FromSeconds(10); // Timeout for waiting operations to complete
            var deadline = DateTime.UtcNow.Add(timeout);

            // Wait for any ongoing operations to complete, with timeout
            while (Interlocked.CompareExchange(ref _activeOperations, 0, 0) > 0)
            {
                // Check if timeout has elapsed
                if (DateTime.UtcNow > deadline)
                {
                    if (Logger.IsWarnEnabled)
                        Logger.Warn($"Timed out waiting for {_activeOperations} operations to complete during worker disposal.");

                    break;
                }

                await Task.Delay(100);
            }

            try
            {
                Stop();
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                if (Logger.IsErrorEnabled)
                    Logger.Error("Error stopping background worker", ex);
            }
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
            internal Action AfterBatchFlushed;
        }
    }
}

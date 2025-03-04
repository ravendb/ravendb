using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.SemanticKernel.Embeddings;
using Raven.Server.Background;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.ETL.Providers.AI;
using Sparrow.Server.Logging;

namespace Raven.Server.Documents.AI.Embeddings
{
    public class EmbeddingsBatchingWorker(string databaseName,
        AiConfiguration configuration,
#pragma warning disable SKEXP0001
        ITextEmbeddingGenerationService service,
#pragma warning restore SKEXP0001
        AiConnectionStringIdentifier connectionStringId,
        SemaphoreSlim concurrencyLimiter,
        RavenLogger logger,
        CancellationToken shutdown)
        : BackgroundWorkBase(databaseName, logger, shutdown)
    {
        private readonly string _databaseName = databaseName;

        private readonly ConcurrentQueue<EmbeddingsBatchRequest> _requestQueue = new();
        private readonly Stopwatch _batchTimer = new();

        public Task<ReadOnlyMemory<float>[]> EnqueueRequestAsync(IList<string> values, CancellationToken cancellationToken)
        {
            var request = new EmbeddingsBatchRequest(values, callerToken: cancellationToken, workerToken: CancellationToken);

            bool wasEmpty = _requestQueue.IsEmpty;
            _requestQueue.Enqueue(request);

            // Start the batch timer if this is the first request
            if (wasEmpty)
                _batchTimer.Restart();

            return request.TaskCompletionSource.Task;
        }

        protected override async Task DoWork()
        {
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
                bool shouldProcessNow = _requestQueue.Count >= configuration.MaxBatchSize ||
                                        _batchTimer.ElapsedMilliseconds >= configuration.BatchTimeoutInMs;

                if (shouldProcessNow == false)
                {
                    // Wait a bit before checking again
                    await WaitOrThrowOperationCanceled(TimeSpan.FromMilliseconds(Math.Min(10, configuration.BatchTimeoutInMs / 5)));
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
                    Logger.Error($"Error in EmbeddingsBatchingWorker for connection '{connectionStringId.Value}' in database '{_databaseName}'", ex);

                // Wait a bit before retrying
                await WaitOrThrowOperationCanceled(TimeSpan.FromSeconds(1));
            }
        }

        private async Task ProcessBatchAsync()
        {
            // Collect requests for this batch (up to MaxBatchSize)
            EmbeddingsBatchRequest[] requestsArray = ArrayPool<EmbeddingsBatchRequest>.Shared.Rent(configuration.MaxBatchSize);

            try
            {
                int count = 0;

                while (count < configuration.MaxBatchSize &&
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
                    // Execute batch with retry logic
                    await FlushBatchAsync(requestsArray, count);
                }
                finally
                {
                    stopwatch.Stop();

                    if (Logger.IsDebugEnabled)
                        Logger.Debug($"Batch processing completed for connection '{connectionStringId.Value}' in {stopwatch.ElapsedMilliseconds}ms, processed {count} requests");
                }
            }
            finally
            {
                ArrayPool<EmbeddingsBatchRequest>.Shared.Return(requestsArray);
            }
        }

        private async Task FlushBatchAsync(EmbeddingsBatchRequest[] requestsArray, int count)
        {
            // First calculate total number of values across all requests
            int totalValueCount = 0;
            for (int i = 0; i < count; i++)
            {
                totalValueCount += requestsArray[i].Values.Count;
            }

            var allTextValues = ArrayPool<string>.Shared.Rent(totalValueCount);

            try
            {
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

                var valuesSegment = new ArraySegment<string>(allTextValues, offset: 0, totalValueCount);

                // Execute with retry logic
                for (int attempt = 0; attempt <= configuration.MaxRetries; attempt++)
                {
                    try
                    {
                        if (attempt > 0)
                        {
                            if (Logger.IsWarnEnabled)
                                Logger.Warn($"Retrying batch for connection '{connectionStringId.Value}', attempt {attempt}/{configuration.MaxRetries}");

                            // Exponential backoff
                            var delay = configuration.RetryDelayMs * Math.Pow(2, attempt - 1);
                            await WaitOrThrowOperationCanceled(TimeSpan.FromMilliseconds((int)delay));
                        }

                        if (Logger.IsDebugEnabled)
                            Logger.Debug($"Processing batch of {totalValueCount} values from {count} requests for connection '{connectionStringId.Value}'");

                        var allEmbeddings = await service.GenerateEmbeddingsAsync(valuesSegment);

                        // Verify we got the expected number of embeddings
                        if (allEmbeddings.Count != totalValueCount)
                        {
                            throw new InvalidOperationException($"Failed to generate embeddings: expected {totalValueCount} embeddings, but got {allEmbeddings.Count}");
                        }

                        // Distribute results back to the requests
                        for (int i = 0; i < count; i++)
                        {
                            var request = requestsArray[i];
                            (int startIndex, int itemsCount) = valueRanges[i];

                            ReadOnlyMemory<float>[] requestEmbeddings = ArrayPool<ReadOnlyMemory<float>>.Shared.Rent(itemsCount);

                            try
                            {
                                // Fill the temp array with embeddings for this request
                                for (int j = 0; j < itemsCount; j++)
                                    requestEmbeddings[j] = allEmbeddings[startIndex + j];

                                // Return the list of embeddings to the caller
                                request.TaskCompletionSource.TrySetResult(requestEmbeddings);
                            }
                            finally
                            {
                                ArrayPool<ReadOnlyMemory<float>>.Shared.Return(requestEmbeddings);
                            }
                        }

                        // Successfully processed batch, exit retry loop
                        break;
                    }
                    catch (Exception ex) when (attempt < configuration.MaxRetries && IsNonRetriableException(ex) == false)
                    {
                        if (Logger.IsWarnEnabled)
                            Logger.Warn($"Error processing batch for connection '{connectionStringId.Value}', retrying {attempt + 1}/{configuration.MaxRetries}", ex);

                        // Continue to next retry iteration
                    }
                    catch (Exception ex)
                    {
                        if (Logger.IsErrorEnabled)
                            Logger.Error($"Final error processing batch for connection '{connectionStringId.Value}' after {attempt} retries", ex);

                        for (int i = 0; i < count; i++)
                            requestsArray[i].TaskCompletionSource.TrySetException(ex);
                    }
                }
            }
            finally
            {
                ArrayPool<string>.Shared.Return(allTextValues);
                ForTestingPurposes?.AfterBatchFlushed?.Invoke();
            }
        }

        private static bool IsNonRetriableException(Exception ex)
        {
            return ex
                is ArgumentException
                or InvalidOperationException
                or UnauthorizedAccessException;
        }

        protected override void InitializeWork()
        {
            if (Logger.IsInfoEnabled)
                Logger.Info($"Initializing {nameof(EmbeddingsBatchingWorker)} for connection '{connectionStringId.Value}' in database '{_databaseName}'");

            _batchTimer.Reset();
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

// -----------------------------------------------------------------------
//  <copyright file="SubscriptionWorker.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

#if !(NETSTANDARD2_0 || NETSTANDARD2_1 || NETCOREAPP2_1 || NETCOREAPP3_1)
#define TCP_CLIENT_CANCELLATIONTOKEN_SUPPORT
#endif

#if !(NETSTANDARD2_0 || NETSTANDARD2_1 || NETCOREAPP2_1)
#define SSL_STREAM_CIPHERSUITESPOLICY_SUPPORT
#endif

using System;
using System.IO;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.ServerWide.Tcp;
using Sparrow.Json;

namespace Raven.Client.Documents.Subscriptions
{
    public class SubscriptionWorker<T> : AbstractSubscriptionWorker<T> where T : class
    {
        private readonly DocumentStore _store;

        /// <summary>
        /// Allows the user to define stuff that happens after the confirm was received from the server
        /// (this way we know we won't get those documents again)
        /// </summary>

        internal SubscriptionWorker(SubscriptionWorkerOptions options, DocumentStore documentStore, string dbName) : base(options, documentStore.GetDatabase(dbName))
        {
            _store = documentStore;
        }

        public Task Run(Action<SubscriptionBatch<T>> processDocuments, CancellationToken ct = default)
        {
            if (processDocuments == null)
                throw new ArgumentNullException(nameof(processDocuments));
            _subscriber = (null, processDocuments);
            return RunInternalAsync(ct);
        }

        public Task Run(Func<SubscriptionBatch<T>, Task> processDocuments, CancellationToken ct = default)
        {
            if (processDocuments == null)
                throw new ArgumentNullException(nameof(processDocuments));
            _subscriber = (processDocuments, null);
            return RunInternalAsync(ct);
        }

        internal override RequestExecutor GetRequestExecutor()
        {
            return _store.GetRequestExecutor(_dbName);
        }

        internal override void SetLocalRequestExecutor(string url, X509Certificate2 cert)
        {
            using (var old = _subscriptionLocalRequestExecutor)
            {
                _subscriptionLocalRequestExecutor = RequestExecutor.CreateForSingleNodeWithoutConfigurationUpdates(url, _dbName, cert, _store.Conventions);
                _store.RegisterEvents(_subscriptionLocalRequestExecutor);
            }
        }

        internal override async Task ProcessSubscriptionInternalAsync(JsonContextPool contextPool, Stream tcpStreamCopy, JsonOperationContext.MemoryBuffer buffer, JsonOperationContext context)
        {
            Task notifiedSubscriber = Task.CompletedTask;

            var batch = new SubscriptionBatch<T>(_subscriptionLocalRequestExecutor, _store, _dbName, _logger);

            while (_processingCts.IsCancellationRequested == false)
            {
                // start reading next batch from server on 1'st thread (can be before client started processing)
                var readFromServer = ReadSingleSubscriptionBatchFromServerAsync(contextPool, tcpStreamCopy, buffer, batch);
                try
                {
                    // wait for the subscriber to complete processing on 2'nd thread
                    await notifiedSubscriber.ConfigureAwait(false);
                }
                catch (Exception)
                {
                    // if the subscriber errored, we shut down
                    try
                    {
                        CloseTcpClient();
                        using ((await readFromServer.ConfigureAwait(false)).ReturnContext)
                        {
                        }
                    }
                    catch (Exception)
                    {
                        // nothing to be done here
                    }

                    throw;
                }

                BatchFromServer incomingBatch = await readFromServer.ConfigureAwait(false); // wait for batch reading to end

                _processingCts.Token.ThrowIfCancellationRequested();

                var lastReceivedChangeVector = batch.Initialize(incomingBatch);

                notifiedSubscriber = Task.Run(async () => // the 2'nd thread
                {
                    // ReSharper disable once AccessToDisposedClosure
                    using (incomingBatch.ReturnContext)
                    {
                        try
                        {
                            if (_subscriber.Async != null)
                                await _subscriber.Async(batch).ConfigureAwait(false);
                            else
                                _subscriber.Sync(batch);
                        }
                        catch (Exception ex)
                        {
                            if (_logger.IsInfoEnabled)
                            {
                                _logger.Info(
                                    $"Subscription '{_options.SubscriptionName}'. Subscriber threw an exception on document batch", ex);
                            }

                            if (_options.IgnoreSubscriberErrors == false)
                                throw new SubscriberErrorException($"Subscriber threw an exception in subscription '{_options.SubscriptionName}'", ex);
                        }
                    }

                    try
                    {
                        if (tcpStreamCopy != null) //possibly prevent ObjectDisposedException
                        {
                            await SendAckAsync(lastReceivedChangeVector, tcpStreamCopy, context, _processingCts.Token).ConfigureAwait(false);
                        }
                    }
                    catch (ObjectDisposedException)
                    {
                        //if this happens, this means we are disposing, so don't care..
                        //(this piece of code happens asynchronously to external using(tcpStream) statement)
                    }
                });
            }
        }
    }
}

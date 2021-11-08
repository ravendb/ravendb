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
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Extensions;
using Raven.Client.Http;

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
            return RunInternal(ct);
        }

        public Task Run(Func<SubscriptionBatch<T>, Task> processDocuments, CancellationToken ct = default)
        {
            if (processDocuments == null)
                throw new ArgumentNullException(nameof(processDocuments));
            _subscriber = (processDocuments, null);
            return RunInternal(ct);
        }

        internal override RequestExecutor GetRequestExecutor()
        {
            return _store.GetRequestExecutor(_dbName);
        }

        internal override SubscriptionBatch<T> GetSubscriptionBatch()
        {
            return new SubscriptionBatch<T>(_subscriptionLocalRequestExecutor, _store, _dbName, _logger);
        }

        internal override Task HandleBatchConfirm()
        {
            return Task.CompletedTask;
        }

        internal override Task HandleSubscriptionError(Exception e)
        {
            return Task.CompletedTask;
        }

        internal override void GetLocalRequestExecutor(string url, X509Certificate2 cert)
        {
            _subscriptionLocalRequestExecutor?.Dispose();
            _subscriptionLocalRequestExecutor = RequestExecutor.CreateForSingleNodeWithoutConfigurationUpdates(url, _dbName, cert, _store.Conventions);
            _store.RegisterEvents(_subscriptionLocalRequestExecutor);
        }
    }
}

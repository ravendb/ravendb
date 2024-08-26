// -----------------------------------------------------------------------
//  <copyright file="SubscriptionWorker.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.Logging;
using Sparrow.Logging;

namespace Raven.Client.Documents.Subscriptions
{
    public sealed class SubscriptionWorker<T> : AbstractSubscriptionWorker<SubscriptionBatch<T>, T> where T : class
    {
        private readonly DocumentStore _store;

        /// <summary>
        /// Allows the user to define stuff that happens after the confirm was received from the server
        /// (this way we know we won't get those documents again)
        /// </summary>

        internal SubscriptionWorker(SubscriptionWorkerOptions options, DocumentStore documentStore, string dbName) : base(options, documentStore.GetDatabase(dbName), RavenLogManager.Instance.GetLoggerForClient<SubscriptionWorker<T>>())
        {
            _store = documentStore;
        }

        protected override RequestExecutor GetRequestExecutor() => _store.GetRequestExecutor(_dbName);

        protected override void SetLocalRequestExecutor(string url, X509Certificate2 cert)
        {
            using (var old = _subscriptionLocalRequestExecutor)
            {
                _subscriptionLocalRequestExecutor = RequestExecutor.CreateForSingleNodeWithoutConfigurationUpdates(url, _dbName, cert, _store.Conventions);
                _store.RegisterEvents(_subscriptionLocalRequestExecutor);
            }
        }

        protected override SubscriptionBatch<T> CreateEmptyBatch() => new SubscriptionBatch<T>(_subscriptionLocalRequestExecutor, _store, _dbName, _logger);

        protected override Task TrySetRedirectNodeOnConnectToServerAsync()
        {
            // no-op
            return Task.CompletedTask;
        }
    }
}

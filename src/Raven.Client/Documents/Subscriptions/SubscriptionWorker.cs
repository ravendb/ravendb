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

using System.Security.Cryptography.X509Certificates;
using Raven.Client.Extensions;
using Raven.Client.Http;

namespace Raven.Client.Documents.Subscriptions
{
    public class SubscriptionWorker<T> : AbstractSubscriptionWorker<SubscriptionBatch<T>, T> where T : class
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

        internal override RequestExecutor GetRequestExecutor() => _store.GetRequestExecutor(_dbName);

        internal override void SetLocalRequestExecutor(string url, X509Certificate2 cert)
        {
            using (var old = _subscriptionLocalRequestExecutor)
            {
                _subscriptionLocalRequestExecutor = RequestExecutor.CreateForSingleNodeWithoutConfigurationUpdates(url, _dbName, cert, _store.Conventions);
                _store.RegisterEvents(_subscriptionLocalRequestExecutor);
            }
        }

        protected override SubscriptionBatch<T> CreateEmptyBatch() => new SubscriptionBatch<T>(_subscriptionLocalRequestExecutor, _store, _dbName, _logger);
    }
}

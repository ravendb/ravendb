using System;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Http;

namespace Raven.Server.Documents.ShardedTcpHandlers
{
    public class ShardedSubscriptionWorker : AbstractSubscriptionWorker<dynamic>
    {
        private readonly RequestExecutor _shardRequestExecutor;
        private readonly Func<Task> _sendClientConfirmTask;
        private readonly Func<Exception, Task> _handleSubscriptionErrorTask;

        public ShardedSubscriptionWorker(SubscriptionWorkerOptions options, string dbName, RequestExecutor re, Func<Task> sendClientConfirmTask, Func<Exception, Task> handleSubscriptionErrorTask, Func<SubscriptionBatch<dynamic>, Task> processDocuments) : base(options, dbName)
        {
            _subscriber = (processDocuments, null);
            _shardRequestExecutor = re; 
            _sendClientConfirmTask = sendClientConfirmTask;
            _handleSubscriptionErrorTask = handleSubscriptionErrorTask;
        }

        internal override RequestExecutor GetRequestExecutor()
        {
            return _shardRequestExecutor;
        }

        internal override SubscriptionBatch<dynamic> GetSubscriptionBatch()
        {
            return new SubscriptionBatch<dynamic>(_subscriptionLocalRequestExecutor, shardWorker: this, _dbName, _logger);
        }

        internal override async Task HandleBatchConfirm()
        {
            await _sendClientConfirmTask();
        }

        internal override async Task HandleSubscriptionError(Exception ex)
        {
            await _handleSubscriptionErrorTask(ex);
        }

        internal override void GetLocalRequestExecutor(string url, X509Certificate2 cert)
        {
            _subscriptionLocalRequestExecutor?.Dispose();
            _subscriptionLocalRequestExecutor = RequestExecutor.CreateForSingleNodeWithoutConfigurationUpdates(url, _dbName, cert, DocumentConventions.Default);
        }
    }
}

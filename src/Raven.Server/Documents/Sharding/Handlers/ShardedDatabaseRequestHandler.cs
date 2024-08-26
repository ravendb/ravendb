using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Exceptions.Database;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Server.Documents.Sharding.Executors;
using Raven.Server.Logging;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils.Configuration;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public abstract partial class ShardedDatabaseRequestHandler : AbstractDatabaseRequestHandler<TransactionOperationContext>
    {
        public ShardedDatabaseContext DatabaseContext;

        public HttpMethod Method;
        public string RelativeShardUrl;
        public string BaseShardUrl;
        public ShardExecutor ShardExecutor => DatabaseContext.ShardExecutor;

        public ShardedContinuationTokensHandler ContinuationTokens;

        public static readonly List<string> HeadersToCopy = new()
        {
            Constants.Headers.LastKnownClusterTransactionIndex
        };

        protected ShardedDatabaseRequestHandler()
        {
            ContinuationTokens = new ShardedContinuationTokensHandler(this);
        }

        private void ModifyHeaders(HttpRequestMessage request)
        {
            foreach (var header in HeadersToCopy)
            {
                if (HttpContext.Request.Headers.TryGetValue(header, out var value))
                {
                    request.Headers.TryAddWithoutValidation(header, (IEnumerable<string>)value);
                }
            }
        }

        public Task<TResult> ExecuteSingleShardAsync<TResult>(JsonOperationContext context, RavenCommand<TResult> command, int shardNumber, CancellationToken token = default)
        {
            command.ModifyRequest = ModifyHeaders;
            return ShardExecutor.ExecuteSingleShardAsync(context, command, shardNumber, token);
        }

        public void InitForOfflineOperation(RequestHandlerContext context)
        {
            base.Init(context);
            DatabaseContext = context.DatabaseContext;
            ContextPool = context.RavenServer.ServerStore.ContextPool;
        }

        public override void Init(RequestHandlerContext context)
        {
            base.Init(context);
            DatabaseContext = context.DatabaseContext;
            //TODO - sharding: We probably want to put it in the ShardedDatabaseContext, not use the server one 
            ContextPool = context.RavenServer.ServerStore.ContextPool;
            Logger = RavenLogManager.Instance.GetLoggerForDatabase(GetType(), DatabaseContext);

            var request = HttpContext.Request;
            var url = context.RouteMatch.Url;

            var relativeIndex = url.IndexOf('/', 11); //start after "/databases/" and skip the database name
            BaseShardUrl = url.Substring(relativeIndex);

            RelativeShardUrl = BaseShardUrl + request.QueryString;
            Method = new HttpMethod(request.Method);

            context.HttpContext.Response.OnStarting(() => CheckForChanges(context));
        }


        public override Task CheckForChanges(RequestHandlerContext context)
        {
            if (context.CheckForChanges == false)
                return Task.CompletedTask;

            var topologyEtag = GetLongFromHeaders(Constants.Headers.TopologyEtag);
            if (topologyEtag.HasValue && DatabaseContext.HasTopologyChanged(topologyEtag.Value))
                context.HttpContext.Response.Headers[Constants.Headers.RefreshTopology] = "true";

            var clientConfigurationEtag = GetLongFromHeaders(Constants.Headers.ClientConfigurationEtag);
            if (clientConfigurationEtag.HasValue && ClientConfigurationHelper.HasClientConfigurationChanged(DatabaseContext.DatabaseRecord.Client, ServerStore, clientConfigurationEtag.Value))
                context.HttpContext.Response.Headers[Constants.Headers.RefreshClientConfiguration] = "true";

            return Task.CompletedTask;
        }

        public override async Task WaitForIndexToBeAppliedAsync(TransactionOperationContext context, long index)
        {
            await ServerStore.Cluster.WaitForIndexNotification(index);

            using (var cts = CreateHttpRequestBoundOperationToken())
            {
                await DatabaseContext.RachisLogIndexNotifications.WaitForIndexNotification(index, cts.Token);

                var dbs = ServerStore.DatabasesLandlord.TryGetOrCreateShardedResourcesStore(DatabaseContext.DatabaseName);

                foreach (var task in dbs)
                {
                    var database = await task.WithCancellation(cts.Token);
                    await database.RachisLogIndexNotifications.WaitForIndexNotification(index, ServerStore.Engine.OperationTimeout).WithCancellation(cts.Token);
                }
            }
        }

        public override bool IsShutdownRequested() => base.IsShutdownRequested() || DatabaseContext.IsShutdownRequested();

        [DoesNotReturn]
        public override void ThrowShutdownException(Exception inner = null) => throw new DatabaseDisabledException("The database " + DatabaseName + " is shutting down", inner);

        public override OperationCancelToken CreateHttpRequestBoundTimeLimitedOperationToken()
        {
            return CreateHttpRequestBoundTimeLimitedOperationToken(DatabaseContext.Configuration.Databases.OperationTimeout.AsTimeSpan);
        }

        public override OperationCancelToken CreateHttpRequestBoundTimeLimitedOperationTokenForQuery()
        {
            return CreateHttpRequestBoundTimeLimitedOperationToken(DatabaseContext.Configuration.Databases.QueryTimeout.AsTimeSpan);
        }

        public override OperationCancelToken CreateHttpRequestBoundTimeLimitedOperationToken(TimeSpan cancelAfter)
        {
            return new OperationCancelToken(cancelAfter, DatabaseContext.DatabaseShutdown, HttpContext.RequestAborted);
        }

        public override OperationCancelToken CreateHttpRequestBoundOperationToken()
        {
            return new OperationCancelToken(DatabaseContext.DatabaseShutdown, HttpContext.RequestAborted);
        }

        public override OperationCancelToken CreateTimeLimitedBackgroundOperationTokenForQueryOperation()
        {
            return new OperationCancelToken(DatabaseContext.Configuration.Databases.QueryOperationTimeout.AsTimeSpan, DatabaseContext.DatabaseShutdown);
        }

        public override OperationCancelToken CreateTimeLimitedBackgroundOperationTokenForCollectionOperation()
        {
            return new OperationCancelToken(DatabaseContext.Configuration.Databases.CollectionOperationTimeout.AsTimeSpan, DatabaseContext.DatabaseShutdown);
        }

        public override OperationCancelToken CreateTimeLimitedBackgroundOperationToken()
        {
            return new OperationCancelToken(DatabaseContext.Configuration.Databases.OperationTimeout.AsTimeSpan, DatabaseContext.DatabaseShutdown);
        }

        public override OperationCancelToken CreateBackgroundOperationToken()
        {
            return new OperationCancelToken(DatabaseContext.DatabaseShutdown);
        }

        public override string DatabaseName => DatabaseContext.DatabaseName;

        public override char IdentityPartsSeparator => DatabaseContext.IdentityPartsSeparator;

        public override async Task WaitForIndexNotificationAsync(long index) => await DatabaseContext.Cluster.WaitForExecutionOnAllNodesAsync(index).ConfigureAwait(false);

        public override bool ShouldAddPagingPerformanceHint(long numberOfResults)
        {
            return numberOfResults > DatabaseContext.Configuration.PerformanceHints.MaxNumberOfResults;
        }

        public override void AddPagingPerformanceHint(PagingOperationType operation, string action, string details, long numberOfResults, long pageSize, long duration,
            long totalDocumentsSizeInBytes)
        {
            if (ShouldAddPagingPerformanceHint(numberOfResults))
                DatabaseContext.NotificationCenter.Paging.Add(operation, action, details, numberOfResults, pageSize, duration, totalDocumentsSizeInBytes);
        }
    }
}

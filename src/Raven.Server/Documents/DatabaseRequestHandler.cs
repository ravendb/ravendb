using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Exceptions.Database;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Server.Logging;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils.Configuration;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.Documents
{
    public abstract class DatabaseRequestHandler : AbstractDatabaseRequestHandler<DocumentsOperationContext>
    {
        public DocumentDatabase Database;

        public override string DatabaseName => Database.Name;

        protected internal delegate void RefAction<T>(string databaseName, ref T configuration, JsonOperationContext context);

        public override void Init(RequestHandlerContext context)
        {
            Database = context.Database;
            ContextPool = Database.DocumentsStorage.ContextPool;
            Logger = RavenLogManager.Instance.GetLoggerForDatabase(GetType(), Database);

            base.Init(context);
        }

        public override char IdentityPartsSeparator => Database.IdentityPartsSeparator;

        public async Task<TResult> ExecuteRemoteAsync<TResult>(RavenCommand<TResult> command, CancellationToken token = default)
        {
            var requestExecutor = Database.RequestExecutor;
            using (requestExecutor.ContextPool.AllocateOperationContext(out JsonOperationContext ctx))
            {
                await requestExecutor.ExecuteAsync(command, ctx, token: token);
                return command.Result;
            }
        }

        public override Task CheckForChanges(RequestHandlerContext context)
        {
            if (context.CheckForChanges == false)
                return Task.CompletedTask;

            var topologyEtag = GetLongFromHeaders(Constants.Headers.TopologyEtag);
            if (topologyEtag.HasValue && Database.HasTopologyChanged(topologyEtag.Value))
                context.HttpContext.Response.Headers[Constants.Headers.RefreshTopology] = "true";

            var clientConfigurationEtag = GetLongFromHeaders(Constants.Headers.ClientConfigurationEtag);
            if (clientConfigurationEtag.HasValue && ClientConfigurationHelper.HasClientConfigurationChanged(Database.ClientConfiguration, ServerStore, clientConfigurationEtag.Value))
                context.HttpContext.Response.Headers[Constants.Headers.RefreshClientConfiguration] = "true";

            return Task.CompletedTask;
        }

        public override async Task WaitForIndexToBeAppliedAsync(TransactionOperationContext context, long index)
        {
            DatabaseTopology dbTopology;
            using (context.OpenReadTransaction())
            {
                dbTopology = ServerStore.Cluster.ReadDatabaseTopology(context, Database.Name);
            }

            if (dbTopology.RelevantFor(ServerStore.NodeTag))
            {
                var db = await ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(Database.Name);
                await db.RachisLogIndexNotifications.WaitForIndexNotification(index, ServerStore.Engine.OperationTimeout);
            }
            else
            {
                await ServerStore.Cluster.WaitForIndexNotification(index);
            }
        }

        public override bool IsShutdownRequested() => base.IsShutdownRequested() || Database.IsShutdownRequested();

        [DoesNotReturn]
        public override void ThrowShutdownException(Exception inner = null) => throw new DatabaseDisabledException("The database " + DatabaseName + " is shutting down", inner);

        public override OperationCancelToken CreateHttpRequestBoundTimeLimitedOperationToken()
        {
            return CreateHttpRequestBoundTimeLimitedOperationToken(Database.Configuration.Databases.OperationTimeout.AsTimeSpan);
        }

        public override OperationCancelToken CreateHttpRequestBoundTimeLimitedOperationTokenForQuery()
        {
            return CreateHttpRequestBoundTimeLimitedOperationToken(Database.Configuration.Databases.QueryTimeout.AsTimeSpan);
        }

        public override OperationCancelToken CreateHttpRequestBoundTimeLimitedOperationToken(TimeSpan cancelAfter)
        {
            return new OperationCancelToken(cancelAfter, Database.DatabaseShutdown, HttpContext.RequestAborted);
        }

        public override OperationCancelToken CreateHttpRequestBoundOperationToken()
        {
            return new OperationCancelToken(Database.DatabaseShutdown, HttpContext.RequestAborted);
        }

        public override OperationCancelToken CreateHttpRequestBoundOperationToken(CancellationToken token)
        {
            return new OperationCancelToken(Database.DatabaseShutdown, HttpContext.RequestAborted, token);
        }

        public override OperationCancelToken CreateTimeLimitedBackgroundOperationTokenForQueryOperation()
        {
            return new OperationCancelToken(Database.Configuration.Databases.QueryOperationTimeout.AsTimeSpan, Database.DatabaseShutdown);
        }

        public override OperationCancelToken CreateTimeLimitedBackgroundOperationTokenForCollectionOperation()
        {
            return new OperationCancelToken(Database.Configuration.Databases.CollectionOperationTimeout.AsTimeSpan, Database.DatabaseShutdown);
        }

        public override OperationCancelToken CreateTimeLimitedBackgroundOperationToken()
        {
            return new OperationCancelToken(Database.Configuration.Databases.OperationTimeout.AsTimeSpan, Database.DatabaseShutdown);
        }
        
        public override OperationCancelToken CreateBackgroundOperationToken()
        {
            return new OperationCancelToken(Database.DatabaseShutdown);
        }

        public override bool ShouldAddPagingPerformanceHint(long numberOfResults)
        {
            return numberOfResults > Database.Configuration.PerformanceHints.MaxNumberOfResults;
        }

        public override void AddPagingPerformanceHint(PagingOperationType operation, string action, string details, long numberOfResults, long pageSize, long duration, long totalDocumentsSizeInBytes)
        {
            if (ShouldAddPagingPerformanceHint(numberOfResults))
                Database.NotificationCenter.Paging.Add(operation, action, details, numberOfResults, pageSize, duration, totalDocumentsSizeInBytes);
        }

        public override Task WaitForIndexNotificationAsync(long index) => Database.RachisLogIndexNotifications.WaitForIndexNotification(index, ServerStore.Engine.OperationTimeout);
    }
}

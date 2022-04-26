using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Util;
using Raven.Client.Exceptions;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils.Configuration;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

namespace Raven.Server.Documents
{
    public abstract class DatabaseRequestHandler : AbstractDatabaseRequestHandler<DocumentsOperationContext>
    {
        public DocumentDatabase Database;
        public Logger Logger;

        public override void Init(RequestHandlerContext context)
        {
            base.Init(context);

            Database = context.Database;
            ContextPool = Database.DocumentsStorage.ContextPool;
            Logger = LoggingSource.Instance.GetLogger(Database.Name, GetType().FullName);

            context.HttpContext.Response.OnStarting(() => CheckForChanges(context));
        }


        public override string DatabaseName => Database.Name;

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

        public Task CheckForChanges(RequestHandlerContext context)
        {
            var topologyEtag = GetLongFromHeaders(Constants.Headers.TopologyEtag);
            if (topologyEtag.HasValue && Database.HasTopologyChanged(topologyEtag.Value))
                context.HttpContext.Response.Headers[Constants.Headers.RefreshTopology] = "true";

            var clientConfigurationEtag = GetLongFromHeaders(Constants.Headers.ClientConfigurationEtag);
            if (clientConfigurationEtag.HasValue && ClientConfigurationHelper.HasClientConfigurationChanged(Database.ClientConfiguration, ServerStore, clientConfigurationEtag.Value))
                context.HttpContext.Response.Headers[Constants.Headers.RefreshClientConfiguration] = "true";

            return Task.CompletedTask;
        }

        protected Task DatabaseConfigurations(SetupFunc<BlittableJsonReaderObject> setupConfigurationFunc,
           string debug,
           string raftRequestId,
           RefAction<BlittableJsonReaderObject> beforeSetupConfiguration = null,
           Action<DynamicJsonValue, BlittableJsonReaderObject, long> fillJson = null,
           HttpStatusCode statusCode = HttpStatusCode.OK)
        {
            return DatabaseConfigurations(
                setupConfigurationFunc,
                debug,
                raftRequestId,
                Database.Name,
                beforeSetupConfiguration,
                fillJson,
                statusCode
            );
        }

        protected Task<(long Index, T Configuration)> DatabaseConfigurations<T>(SetupFunc<T> setupConfigurationFunc, TransactionOperationContext context, string raftRequestId, T configurationJson, RefAction<T> beforeSetupConfiguration = null)
        {
            return DatabaseConfigurations(setupConfigurationFunc, context, raftRequestId, Database.Name, configurationJson, beforeSetupConfiguration);
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

        public override OperationCancelToken CreateTimeLimitedOperationToken()
        {
            return new OperationCancelToken(Database.Configuration.Databases.OperationTimeout.AsTimeSpan, Database.DatabaseShutdown, HttpContext.RequestAborted);
        }

        protected OperationCancelToken CreateTimeLimitedQueryToken()
        {
            return new OperationCancelToken(Database.Configuration.Databases.QueryTimeout.AsTimeSpan, Database.DatabaseShutdown, HttpContext.RequestAborted);
        }

        protected OperationCancelToken CreateTimeLimitedCollectionOperationToken()
        {
            return new OperationCancelToken(Database.Configuration.Databases.CollectionOperationTimeout.AsTimeSpan, Database.DatabaseShutdown, HttpContext.RequestAborted);
        }

        public OperationCancelToken CreateTimeLimitedQueryOperationToken()
        {
            return new OperationCancelToken(Database.Configuration.Databases.QueryOperationTimeout.AsTimeSpan, Database.DatabaseShutdown, HttpContext.RequestAborted);
        }

        public override OperationCancelToken CreateOperationToken()
        {
            return new OperationCancelToken(Database.DatabaseShutdown, HttpContext.RequestAborted);
        }

        public override OperationCancelToken CreateOperationToken(TimeSpan cancelAfter)
        {
            return new OperationCancelToken(cancelAfter, Database.DatabaseShutdown, HttpContext.RequestAborted);
        }

        public override bool ShouldAddPagingPerformanceHint(long numberOfResults)
        {
            return numberOfResults > Database.Configuration.PerformanceHints.MaxNumberOfResults;
        }

        public override void AddPagingPerformanceHint(PagingOperationType operation, string action, string details, long numberOfResults, int pageSize, long duration, long totalDocumentsSizeInBytes)
        {
            if (ShouldAddPagingPerformanceHint(numberOfResults))
                Database.NotificationCenter.Paging.Add(operation, action, details, numberOfResults, pageSize, duration, totalDocumentsSizeInBytes);
        }

        public override Task WaitForIndexNotificationAsync(long index) => Database.RachisLogIndexNotifications.WaitForIndexNotification(index, ServerStore.Engine.OperationTimeout);
    }
}

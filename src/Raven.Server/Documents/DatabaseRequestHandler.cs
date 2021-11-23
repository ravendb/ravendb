using System;
using System.Net;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Changes;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide;
using Raven.Client.Util;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

namespace Raven.Server.Documents
{
    public abstract class DatabaseRequestHandler : RequestHandler
    {
        protected DocumentsContextPool ContextPool;
        protected DocumentDatabase Database;
        protected Logger Logger;

        public override void Init(RequestHandlerContext context)
        {
            base.Init(context);

            Database = context.Database;
            ContextPool = Database.DocumentsStorage.ContextPool;
            Logger = LoggingSource.Instance.GetLogger(Database.Name, GetType().FullName);

            context.HttpContext.Response.OnStarting(() => CheckForChanges(context));
        }

        public Task CheckForChanges(RequestHandlerContext context)
        {
            var topologyEtag = GetLongFromHeaders(Constants.Headers.TopologyEtag);
            if (topologyEtag.HasValue && Database.HasTopologyChanged(topologyEtag.Value))
                context.HttpContext.Response.Headers[Constants.Headers.RefreshTopology] = "true";

            var clientConfigurationEtag = GetLongFromHeaders(Constants.Headers.ClientConfigurationEtag);
            if (clientConfigurationEtag.HasValue && Database.HasClientConfigurationChanged(clientConfigurationEtag.Value))
                context.HttpContext.Response.Headers[Constants.Headers.RefreshClientConfiguration] = "true";

            return Task.CompletedTask;
        }

        protected delegate void RefAction<T>(string databaseName, ref T configuration, JsonOperationContext context);

        protected delegate Task<(long, object)> SetupFunc<T>(TransactionOperationContext context, string databaseName, T json, string raftRequestId);

        protected async Task DatabaseConfigurations(SetupFunc<BlittableJsonReaderObject> setupConfigurationFunc,
           string debug,
           string raftRequestId,
           RefAction<BlittableJsonReaderObject> beforeSetupConfiguration = null,
           Action<DynamicJsonValue, BlittableJsonReaderObject, long> fillJson = null,
           HttpStatusCode statusCode = HttpStatusCode.OK)
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var configurationJson = await context.ReadForMemoryAsync(RequestBodyStream(), debug);
                var result = await DatabaseConfigurations(setupConfigurationFunc, context, raftRequestId, configurationJson, beforeSetupConfiguration);

                if (result.Configuration == null)
                    return;

                HttpContext.Response.StatusCode = (int)statusCode;

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    var json = new DynamicJsonValue
                    {
                        ["RaftCommandIndex"] = result.Index
                    };
                    fillJson?.Invoke(json, result.Configuration, result.Index);
                    context.Write(writer, json);
                }
            }
        }


        protected async Task<(long Index, T Configuration)> DatabaseConfigurations<T>(SetupFunc<T> setupConfigurationFunc, TransactionOperationContext context,  string raftRequestId, T configurationJson, RefAction<T> beforeSetupConfiguration = null)
        {
            if (await CanAccessDatabaseAsync(Database.Name, requireAdmin: true, requireWrite: true) == false)
                return (-1, default);

            if (ResourceNameValidator.IsValidResourceName(Database.Name, ServerStore.Configuration.Core.DataDirectory.FullPath, out string errorMessage) == false)
                throw new BadRequestException(errorMessage);

            await ServerStore.EnsureNotPassiveAsync();

            beforeSetupConfiguration?.Invoke(Database.Name, ref configurationJson, context);

            var (index, _) = await setupConfigurationFunc(context, Database.Name, configurationJson, raftRequestId);
            await WaitForIndexToBeApplied(context, index);

            return (index, configurationJson);
        }

        protected async Task WaitForIndexToBeApplied(TransactionOperationContext context, long index)
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

        /// <summary>
        /// puts the given string in TrafficWatch property of HttpContext.Items
        /// puts the given type in TrafficWatchChangeType property of HttpContext.Items
        /// </summary>
        /// <param name="str"></param>
        /// <param name="type"></param>
        public void AddStringToHttpContext(string str, TrafficWatchChangeType type)
        {
            HttpContext.Items["TrafficWatch"] = (str, type);
        }

        protected OperationCancelToken CreateTimeLimitedOperationToken()
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

        protected OperationCancelToken CreateTimeLimitedQueryOperationToken()
        {
            return new OperationCancelToken(Database.Configuration.Databases.QueryOperationTimeout.AsTimeSpan, Database.DatabaseShutdown, HttpContext.RequestAborted);
        }

        protected override OperationCancelToken CreateOperationToken()
        {
            return new OperationCancelToken(Database.DatabaseShutdown, HttpContext.RequestAborted);
        }

        protected override OperationCancelToken CreateOperationToken(TimeSpan cancelAfter)
        {
            return new OperationCancelToken(cancelAfter, Database.DatabaseShutdown, HttpContext.RequestAborted);
        }

        protected bool ShouldAddPagingPerformanceHint(long numberOfResults)
        {
            return numberOfResults > Database.Configuration.PerformanceHints.MaxNumberOfResults;
        }
        
        protected void AddPagingPerformanceHint(PagingOperationType operation, string action, string details, long numberOfResults, int pageSize, long duration, long totalDocumentsSizeInBytes)
        {
            if(ShouldAddPagingPerformanceHint(numberOfResults))
                Database.NotificationCenter.Paging.Add(operation, action, details, numberOfResults, pageSize, duration, totalDocumentsSizeInBytes);
        }
    }
}

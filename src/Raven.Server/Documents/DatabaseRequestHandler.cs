using System;
using System.Net;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Exceptions;
using Raven.Client.Json.Serialization;
using Raven.Client.ServerWide;
using Raven.Client.Util;
using Raven.Server.Documents.Handlers;
using Raven.Server.Json;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Raven.Server.Web.System;
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
            Database = context.Database;
            ContextPool = Database.DocumentsStorage.ContextPool;
            Logger = LoggingSource.Instance.GetLogger(Database.Name, GetType().FullName);

            base.Init(context);
        }

        public override Task CheckForChanges(RequestHandlerContext context)
        {
            if (context.CheckForChanges == false)
                return Task.CompletedTask;

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

                LogTaskToAudit(debug, result.Index, result.Configuration);
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

        protected OperationCancelToken CreateHttpRequestBoundTimeLimitedOperationToken()
        {
            return CreateHttpRequestBoundTimeLimitedOperationToken(Database.Configuration.Databases.OperationTimeout.AsTimeSpan);
        }

        protected OperationCancelToken CreateHttpRequestBoundTimeLimitedOperationTokenForQuery()
        {
            return CreateHttpRequestBoundTimeLimitedOperationToken(Database.Configuration.Databases.QueryTimeout.AsTimeSpan);
        }

        protected override OperationCancelToken CreateHttpRequestBoundTimeLimitedOperationToken(TimeSpan cancelAfter)
        {
            return new OperationCancelToken(cancelAfter, Database.DatabaseShutdown, HttpContext.RequestAborted);
        }

        protected override OperationCancelToken CreateHttpRequestBoundOperationToken()
        {
            return new OperationCancelToken(Database.DatabaseShutdown, HttpContext.RequestAborted);
        }

        protected OperationCancelToken CreateTimeLimitedBackgroundOperationTokenForQueryOperation()
        {
            return new OperationCancelToken(Database.Configuration.Databases.QueryOperationTimeout.AsTimeSpan, Database.DatabaseShutdown);
        }

        protected OperationCancelToken CreateTimeLimitedBackgroundOperationTokenForCollectionOperation()
        {
            return new OperationCancelToken(Database.Configuration.Databases.CollectionOperationTimeout.AsTimeSpan, Database.DatabaseShutdown);
        }

        protected OperationCancelToken CreateTimeLimitedBackgroundOperationToken()
        {
            return new OperationCancelToken(Database.Configuration.Databases.OperationTimeout.AsTimeSpan, Database.DatabaseShutdown);
        }
        
        protected override OperationCancelToken CreateBackgroundOperationToken()
        {
            return new OperationCancelToken(Database.DatabaseShutdown);
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

        private DynamicJsonValue GetCustomConfigurationAuditJson(string name, BlittableJsonReaderObject configuration)
        {
            switch (name)
            {
                case RevisionsHandler.ReadRevisionsConfigTag:
                    return JsonDeserializationServer.RevisionsConfiguration(configuration).ToAuditJson();

                case RevisionsHandler.ConflictedRevisionsConfigTag:
                    return JsonDeserializationServer.RevisionsCollectionConfiguration(configuration).ToAuditJson();

                case OngoingTasksHandler.BackupDatabaseOnceTag:
                    return JsonDeserializationServer.BackupConfiguration(configuration).ToAuditJson();

                case OngoingTasksHandler.UpdatePeriodicBackupDebugTag:
                    return JsonDeserializationClient.PeriodicBackupConfiguration(configuration).ToAuditJson();

                case OngoingTasksHandler.UpdateExternalReplicationDebugTag:
                    return JsonDeserializationClient.ExternalReplication(configuration).ToAuditJson();

                case PullReplicationHandler.DefineHubDebugTag:
                    return JsonDeserializationClient.PullReplicationDefinition(configuration).ToAuditJson();

                case PullReplicationHandler.UpdatePullReplicationOnSinkNodeDebugTag:
                    return JsonDeserializationClient.PullReplicationAsSink(configuration).ToAuditJson();

                case OngoingTasksHandler.AddEtlDebugTag:
                    return GetEtlConfigurationAuditJson(configuration);

                case OngoingTasksHandler.PutConnectionStringDebugTag:
                    return GetConnectionStringConfigurationAuditJson(configuration);
            }
            return null;
        }

        private DynamicJsonValue GetEtlConfigurationAuditJson(BlittableJsonReaderObject configuration)
        {
            var etlType = EtlConfiguration<ConnectionString>.GetEtlType(configuration);

            switch (etlType)
            {
                case EtlType.Raven:
                    return JsonDeserializationClient.RavenEtlConfiguration(configuration).ToAuditJson();

                case EtlType.ElasticSearch:
                    return JsonDeserializationClient.ElasticSearchEtlConfiguration(configuration).ToAuditJson();

                case EtlType.Queue:
                    return JsonDeserializationClient.QueueEtlConfiguration(configuration).ToAuditJson();

                case EtlType.Sql:
                    return JsonDeserializationClient.SqlEtlConfiguration(configuration).ToAuditJson();

                case EtlType.Olap:
                    return JsonDeserializationClient.OlapEtlConfiguration(configuration).ToAuditJson();
            }

            return null;
        }

        private DynamicJsonValue GetConnectionStringConfigurationAuditJson(BlittableJsonReaderObject configuration)
        {
            var connectionStringType = ConnectionString.GetConnectionStringType(configuration);
            switch (connectionStringType)
            {
                case ConnectionStringType.Raven:
                    return JsonDeserializationClient.RavenConnectionString(configuration).ToAuditJson();
                
                case ConnectionStringType.ElasticSearch:
                    return JsonDeserializationClient.ElasticSearchConnectionString(configuration).ToAuditJson();

                case ConnectionStringType.Queue:
                    return JsonDeserializationClient.QueueConnectionString(configuration).ToAuditJson();

                case ConnectionStringType.Sql:
                    return JsonDeserializationClient.SqlConnectionString(configuration).ToAuditJson();

                case ConnectionStringType.Olap:
                    return JsonDeserializationClient.OlapConnectionString(configuration).ToAuditJson();
            }

            return null;
        }

        protected void LogTaskToAudit(string description, long id, BlittableJsonReaderObject configuration)
        {
            if (LoggingSource.AuditLog.IsInfoEnabled)
            {
                DynamicJsonValue conf = GetCustomConfigurationAuditJson(description, configuration);
                var line = $"'{description}' with taskId: '{id}'";

                if (conf != null)
                {
                    string confString;
                    using (ContextPool.AllocateOperationContext(out JsonOperationContext ctx))
                    {
                        confString = ctx.ReadObject(conf, "conf").ToString();
                    }

                    line += ($" Configuration: {confString}");
                }

                LogAuditFor(Database.Name, "TASK", line);
            }
        }
    }
}

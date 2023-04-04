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
using Raven.Server.Json;
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

                var id ="";
                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    var json = new DynamicJsonValue
                    {
                        ["RaftCommandIndex"] = result.Index
                    };
                    fillJson?.Invoke(json, result.Configuration, result.Index);
                    context.Write(writer, json);
                    id = json["TaskId"] == null ? json["RaftCommandIndex"].ToString() : json["TaskId"].ToString();

                }
                DynamicJsonValue configuration = GetCustomConfigurationAuditJson(debug,result.Configuration);
                LogTaskToAudit(debug, id, configuration);
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

        protected DynamicJsonValue GetCustomConfigurationAuditJson(string name, BlittableJsonReaderObject configuration)
        {
            switch (name)
            {
                case "update-periodic-backup":
                    return JsonDeserializationServer.BackupConfiguration(configuration).ToAuditJson();
                case "etl-add":
                    var etlType = EtlConfiguration<ConnectionString>.GetEtlType(configuration).ToString();
                    if (etlType == "Raven")
                        return JsonDeserializationClient.RavenEtlConfiguration(configuration).ToJson();
                    if (etlType == "ElasticSearch")
                        return JsonDeserializationClient.ElasticSearchEtlConfiguration(configuration).ToJson();
                    if (etlType == "Queue")
                        return JsonDeserializationClient.QueueEtlConfiguration(configuration).ToJson();
                    if (etlType == "Sql")
                        return JsonDeserializationClient.SqlEtlConfiguration(configuration).ToJson();
                    if (etlType == "Olap")
                        return JsonDeserializationClient.OlapEtlConfiguration(configuration).ToJson();
                    break;
                case "update_external_replication":
                    return JsonDeserializationClient.ExternalReplication(configuration).ToJson();
                case "update-hub-pull-replication":
                    return JsonDeserializationClient.PullReplicationDefinition(configuration).ToAuditJson();
                case "update-sink-pull-replication":
                    return JsonDeserializationClient.PullReplicationAsSink(configuration).ToAuditJson();
                case "put-connection-string":
                    var connectionStringType = ConnectionString.GetConnectionStringType(configuration).ToString();
                    if (connectionStringType == "Raven")
                        return JsonDeserializationClient.RavenConnectionString(configuration).ToJson();
                    if (connectionStringType == "ElasticSearch")
                        return JsonDeserializationClient.ElasticSearchConnectionString(configuration).ToAuditJson();
                    if (connectionStringType == "Queue")
                        return JsonDeserializationClient.QueueConnectionString(configuration).ToJson();
                    if (connectionStringType == "Sql")
                        return JsonDeserializationClient.SqlConnectionString(configuration).ToJson();
                    if (connectionStringType == "Olap")
                        return JsonDeserializationClient.OlapConnectionString(configuration).ToJson();
                    break;
            }
            return null;
        }

        protected void LogTaskToAudit(string description, string id, DynamicJsonValue conf)
        {
            if (LoggingSource.AuditLog.IsInfoEnabled)
            {
                var clientCert = GetCurrentCertificate();
                var auditLog = LoggingSource.AuditLog.GetLogger(Database.Name, "Audit");
                var line = $"Task: {description} with taskId: {id} executed by {clientCert?.Subject} {clientCert?.Thumbprint} ";

                if (conf != null)
                {
                    var confString = "";
                    using (ContextPool.AllocateOperationContext(out JsonOperationContext ctx))
                    {
                        confString = ctx.ReadObject(conf, "conf").ToString();
                    }
                    line += ($"Configuration: {confString}");
                }
                auditLog.Info(line);
            }
        }
    }
}

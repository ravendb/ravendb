using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Exceptions;
using Raven.Client.Util;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.OngoingTasks
{
    internal sealed class OngoingTasksHandlerProcessorForGetConnectionString<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        public OngoingTasksHandlerProcessorForGetConnectionString([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        public override async ValueTask ExecuteAsync()
        {
            if (ResourceNameValidator.IsValidResourceName(RequestHandler.DatabaseName, RequestHandler.ServerStore.Configuration.Core.DataDirectory.FullPath, out string errorMessage) == false)
                throw new BadRequestException(errorMessage);

            if (await RequestHandler.CanAccessDatabaseAsync(RequestHandler.DatabaseName, requireAdmin: true, requireWrite: false) == false)
                return;

            var connectionStringName = RequestHandler.GetStringQueryString("connectionStringName", false);
            var typeString = RequestHandler.GetStringQueryString("type", false);

            await RequestHandler.ServerStore.EnsureNotPassiveAsync();
            RequestHandler.HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;

            using (RequestHandler.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                GetConnectionStringsResult connectionStrings;

                using (context.OpenReadTransaction())
                using (var rawRecord = RequestHandler.ServerStore.Cluster.ReadRawDatabaseRecord(context, RequestHandler.DatabaseName))
                {
                    if (connectionStringName != null)
                    {
                        if (string.IsNullOrWhiteSpace(connectionStringName))
                            throw new BadRequestException($"'{nameof(connectionStringName)}' must have a non empty value");

                        if (Enum.TryParse(typeString, ignoreCase: true, out ConnectionStringType connectionStringType) == false)
                            throw new BadRequestException($"Unknown connection string type: {typeString}");

                        connectionStrings = rawRecord.GetConnectionString(connectionStringName, connectionStringType);
                    }
                    else
                    {
                        connectionStrings = rawRecord.GetConnectionStrings();
                    }

                    AssignUsedBy(connectionStrings, rawRecord);
                }

                await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                {
                    context.Write(writer, connectionStrings.ToJson());
                }
            }
        }

        private static void AssignUsedBy(GetConnectionStringsResult result, RawDatabaseRecord rawRecord)
        {
            var usageMap = BuildUsageMap(rawRecord);

            AssignToDict(result.RavenConnectionStrings);
            AssignToDict(result.SqlConnectionStrings);
            AssignToDict(result.OlapConnectionStrings);
            AssignToDict(result.ElasticSearchConnectionStrings);
            AssignToDict(result.QueueConnectionStrings);
            AssignToDict(result.SnowflakeConnectionStrings);
            AssignToDict(result.AiConnectionStrings);

            void AssignToDict<T>(Dictionary<string, T> dict) where T : ConnectionString
            {
                if (dict == null)
                    return;
                foreach (var cs in dict.Values)
                {
                    if (usageMap.TryGetValue(cs.Name, out var usages))
                        cs.UsedBy = usages;
                }
            }
        }

        private static Dictionary<string, List<ConnectionStringUsage>> BuildUsageMap(RawDatabaseRecord rawRecord)
        {
            var map = new Dictionary<string, List<ConnectionStringUsage>>(StringComparer.OrdinalIgnoreCase);

            void AddTask(string connectionStringName, ConnectionStringUsageKind kind, long taskId, string taskName) =>
                Add(connectionStringName, new ConnectionStringUsage { Kind = kind, Id = taskId, Name = taskName });

            void AddAgent(string connectionStringName, string identifier, string agentName) =>
                Add(connectionStringName, new ConnectionStringUsage { Kind = ConnectionStringUsageKind.AiAgent, Identifier = identifier, Name = agentName });

            void Add(string connectionStringName, ConnectionStringUsage usage)
            {
                if (connectionStringName == null)
                    return;
                if (map.TryGetValue(connectionStringName, out var list) == false)
                    map[connectionStringName] = list = new List<ConnectionStringUsage>();
                list.Add(usage);
            }

            foreach (var t in rawRecord.RavenEtls) AddTask(t.ConnectionStringName, ConnectionStringUsageKind.RavenEtl, t.TaskId, t.Name);
            foreach (var t in rawRecord.SqlEtls) AddTask(t.ConnectionStringName, ConnectionStringUsageKind.SqlEtl, t.TaskId, t.Name);
            foreach (var t in rawRecord.OlapEtls) AddTask(t.ConnectionStringName, ConnectionStringUsageKind.OlapEtl, t.TaskId, t.Name);
            foreach (var t in rawRecord.ElasticSearchEtls) AddTask(t.ConnectionStringName, ConnectionStringUsageKind.ElasticSearchEtl, t.TaskId, t.Name);
            foreach (var t in rawRecord.QueueEtls) AddTask(t.ConnectionStringName, ConnectionStringUsageKind.QueueEtl, t.TaskId, t.Name);
            foreach (var t in rawRecord.SnowflakeEtls) AddTask(t.ConnectionStringName, ConnectionStringUsageKind.SnowflakeEtl, t.TaskId, t.Name);
            foreach (var t in rawRecord.QueueSinks) AddTask(t.ConnectionStringName, ConnectionStringUsageKind.QueueSink, t.TaskId, t.Name);
            foreach (var t in rawRecord.EmbeddingsGenerations) AddTask(t.ConnectionStringName, ConnectionStringUsageKind.EmbeddingsGeneration, t.TaskId, t.Name);
            foreach (var t in rawRecord.GenAis) AddTask(t.ConnectionStringName, ConnectionStringUsageKind.GenAi, t.TaskId, t.Name);
            foreach (var t in rawRecord.ExternalReplications) AddTask(t.ConnectionStringName, ConnectionStringUsageKind.ExternalReplication, t.TaskId, t.Name);
            foreach (var t in rawRecord.SinkPullReplications) AddTask(t.ConnectionStringName, ConnectionStringUsageKind.PullReplicationAsSink, t.TaskId, t.Name);
            foreach (var a in rawRecord.AiAgents) AddAgent(a.ConnectionStringName, a.Identifier, a.Name);

            return map;
        }
    }
}

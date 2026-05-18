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

                    AssignUsedByTasks(connectionStrings, rawRecord);
                }

                await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                {
                    context.Write(writer, connectionStrings.ToJson());
                }
            }
        }

        private static void AssignUsedByTasks(GetConnectionStringsResult result, RawDatabaseRecord rawRecord)
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
                        cs.UsedByTasks = usages;
                }
            }
        }

        private static Dictionary<string, List<ConnectionStringTaskUsage>> BuildUsageMap(RawDatabaseRecord rawRecord)
        {
            var map = new Dictionary<string, List<ConnectionStringTaskUsage>>(StringComparer.OrdinalIgnoreCase);

            void Add(string connectionStringName, long taskId, string taskName)
            {
                if (connectionStringName == null)
                    return;
                if (map.TryGetValue(connectionStringName, out var list) == false)
                    map[connectionStringName] = list = new List<ConnectionStringTaskUsage>();
                list.Add(new ConnectionStringTaskUsage { TaskId = taskId, TaskName = taskName });
            }

            foreach (var t in rawRecord.RavenEtls) Add(t.ConnectionStringName, t.TaskId, t.Name);
            foreach (var t in rawRecord.SqlEtls) Add(t.ConnectionStringName, t.TaskId, t.Name);
            foreach (var t in rawRecord.OlapEtls) Add(t.ConnectionStringName, t.TaskId, t.Name);
            foreach (var t in rawRecord.ElasticSearchEtls) Add(t.ConnectionStringName, t.TaskId, t.Name);
            foreach (var t in rawRecord.QueueEtls) Add(t.ConnectionStringName, t.TaskId, t.Name);
            foreach (var t in rawRecord.SnowflakeEtls) Add(t.ConnectionStringName, t.TaskId, t.Name);
            foreach (var t in rawRecord.QueueSinks) Add(t.ConnectionStringName, t.TaskId, t.Name);
            foreach (var t in rawRecord.EmbeddingsGenerations) Add(t.ConnectionStringName, t.TaskId, t.Name);
            foreach (var t in rawRecord.ExternalReplications) Add(t.ConnectionStringName, t.TaskId, t.Name);
            foreach (var t in rawRecord.SinkPullReplications) Add(t.ConnectionStringName, t.TaskId, t.Name);

            return map;
        }
    }
}

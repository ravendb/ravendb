using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide;
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
            var usageMap = BuildUsageMap(rawRecord.MaterializedRecord);

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

        private static Dictionary<string, List<ConnectionStringUsage>> BuildUsageMap(DatabaseRecord record)
        {
            var map = new Dictionary<string, List<ConnectionStringUsage>>(StringComparer.OrdinalIgnoreCase);

            foreach (var usage in ConnectionStringConsumers.GetUsages(record))
            {
                if (usage.ConnectionStringName == null)
                    continue;
                if (map.TryGetValue(usage.ConnectionStringName, out var list) == false)
                    map[usage.ConnectionStringName] = list = new List<ConnectionStringUsage>();
                list.Add(new ConnectionStringUsage { Kind = usage.Kind, Id = usage.Id, Identifier = usage.Identifier, Name = usage.Name });
            }

            return map;
        }
    }
}

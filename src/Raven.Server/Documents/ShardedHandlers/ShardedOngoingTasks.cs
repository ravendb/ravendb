// -----------------------------------------------------------------------
//  <copyright file="ShardedOngoingTasksHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Nito.AsyncEx;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Exceptions;
using Raven.Client.Util;
using Raven.Server.Documents.Sharding;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ShardedHandlers
{
    public class ShardedOngoingTasksHandler : ShardedRequestHandler
    {
        [RavenShardedAction("/databases/*/admin/connection-strings", "PUT")]
        public async Task PutConnectionString()
        {
            await DatabaseConfigurations((_, databaseName, connectionString, guid) => ServerStore.PutConnectionString(_, databaseName, connectionString, guid), 
                "put-connection-string", GetRaftRequestIdFromQuery());
        }



        [RavenShardedAction("/databases/*/admin/etl", "PUT")]
        public async Task AddEtl()
        {
            var id = GetLongQueryString("id", required: false);

            if (id == null)
            {
                await DatabaseConfigurations((_, databaseName, etlConfiguration, guid) =>
                        ServerStore.AddEtl(_, databaseName, etlConfiguration, guid), "etl-add",
                    GetRaftRequestIdFromQuery(),
                    beforeSetupConfiguration: AssertCanAddOrUpdateEtl,
                    fillJson: (json, _, index) => json[nameof(EtlConfiguration<ConnectionString>.TaskId)] = index);

                return;
            }

            string etlConfigurationName = null;

            await DatabaseConfigurations((_, databaseName, etlConfiguration, guid) =>
                {
                    var task = ServerStore.UpdateEtl(_, databaseName, id.Value, etlConfiguration, guid);
                    etlConfiguration.TryGet(nameof(RavenEtlConfiguration.Name), out etlConfigurationName);
                    return task;
                }, "etl-update",
                GetRaftRequestIdFromQuery(),
                beforeSetupConfiguration: AssertCanAddOrUpdateEtl,
                fillJson: (json, _, index) => json[nameof(EtlConfiguration<ConnectionString>.TaskId)] = index);

            // Reset scripts if needed
            var scriptsToReset = HttpContext.Request.Query["reset"];
            var raftRequestId = GetRaftRequestIdFromQuery();

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                foreach (var script in scriptsToReset)
                {
                    await ServerStore.RemoveEtlProcessState(ctx, ShardedContext.DatabaseName, etlConfigurationName, script, $"{raftRequestId}/{script}");
                }
            }
        }

        protected delegate void RefAction(string databaseName, ref BlittableJsonReaderObject configuration, JsonOperationContext context);

        protected delegate Task<(long, object)> SetupFunc(TransactionOperationContext context, string databaseName, BlittableJsonReaderObject json, string raftRequestId);

        protected async Task DatabaseConfigurations(SetupFunc setupConfigurationFunc,
           string debug,
           string raftRequestId,
           RefAction beforeSetupConfiguration = null,
           Action<DynamicJsonValue, BlittableJsonReaderObject, long> fillJson = null,
           HttpStatusCode statusCode = HttpStatusCode.OK)
        {
            if (await CanAccessDatabaseAsync(ShardedContext.DatabaseName, requireAdmin: true, requireWrite: true) == false)
                return;

            if (ResourceNameValidator.IsValidResourceName(ShardedContext.DatabaseName, ServerStore.Configuration.Core.DataDirectory.FullPath, out string errorMessage) == false)
                throw new BadRequestException(errorMessage);

            await ServerStore.EnsureNotPassiveAsync();
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var configurationJson = await context.ReadForMemoryAsync(RequestBodyStream(), debug);
                beforeSetupConfiguration?.Invoke(ShardedContext.DatabaseName, ref configurationJson, context);

                var (index, _) = await setupConfigurationFunc(context, ShardedContext.DatabaseName, configurationJson, raftRequestId);
                await WaitForIndexToBeApplied(context, index);
                HttpContext.Response.StatusCode = (int)statusCode;

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    var json = new DynamicJsonValue
                    {
                        ["RaftCommandIndex"] = index,
                    };
                    fillJson?.Invoke(json, configurationJson, index);
                    context.Write(writer, json);
                }
            }
        }

        protected async Task WaitForIndexToBeApplied(TransactionOperationContext context, long index)
        {
            var dbs = ServerStore.DatabasesLandlord.TryGetOrCreateShardedResourcesStore(ShardedContext.DatabaseName).ToList();
            if (dbs.Count == 0)
            {
                await ServerStore.Cluster.WaitForIndexNotification(index);
            }
            else
            {
                var tasks = new List<Task>();
                foreach (var task in dbs)
                {
                    var db = await task;
                    tasks.Add(db.RachisLogIndexNotifications.WaitForIndexNotification(index, ServerStore.Engine.OperationTimeout));
                }
                await tasks.WhenAll();
            }
        }

        private void AssertCanAddOrUpdateEtl(string databaseName, ref BlittableJsonReaderObject etlConfiguration, JsonOperationContext context)
        {
            switch (EtlConfiguration<ConnectionString>.GetEtlType(etlConfiguration))
            {
                case EtlType.Raven:
                    ServerStore.LicenseManager.AssertCanAddRavenEtl();
                    break;
                case EtlType.Sql:
                    ServerStore.LicenseManager.AssertCanAddSqlEtl();
                    break;
                case EtlType.Olap:
                    ServerStore.LicenseManager.AssertCanAddOlapEtl();
                    break;
                default:
                    throw new NotSupportedException($"Unknown ETL configuration type. Configuration: {etlConfiguration}");
            }
        }

        /*        protected delegate void RefAction(string databaseName, ref BlittableJsonReaderObject configuration, JsonOperationContext context);


                protected delegate Task<(long, object)> SetupFunc(TransactionOperationContext context, string databaseName, BlittableJsonReaderObject json, string raftRequestId);


                protected async Task DatabaseConfigurations(SetupFunc setupConfigurationFunc,
                    string debug,
                    string raftRequestId,
                    RefAction beforeSetupConfiguration = null,
                    Action<DynamicJsonValue, BlittableJsonReaderObject, long> fillJson = null,
                    HttpStatusCode statusCode = HttpStatusCode.OK)
                {
                    if (await CanAccessDatabaseAsync(Database.Name, requireAdmin: true, requireWrite: true) == false)
                        return;

                    if (ResourceNameValidator.IsValidResourceName(Database.Name, ServerStore.Configuration.Core.DataDirectory.FullPath, out string errorMessage) == false)
                        throw new BadRequestException(errorMessage);

                    await ServerStore.EnsureNotPassiveAsync();
                    using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                    {
                        var configurationJson = await context.ReadForMemoryAsync(RequestBodyStream(), debug);
                        beforeSetupConfiguration?.Invoke(Database.Name, ref configurationJson, context);

                        var (index, _) = await setupConfigurationFunc(context, Database.Name, configurationJson, raftRequestId);
                        await WaitForIndexToBeApplied(context, index);
                        HttpContext.Response.StatusCode = (int)statusCode;

                        await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                        {
                            var json = new DynamicJsonValue
                            {
                                ["RaftCommandIndex"] = index,
                            };
                            fillJson?.Invoke(json, configurationJson, index);
                            context.Write(writer, json);
                        }
                    }
                }*/
    }
}

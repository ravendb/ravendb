using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Exceptions;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Web
{
    public abstract partial class RequestHandler
    {
        protected internal delegate void RefAction<T>(string databaseName, ref T configuration, JsonOperationContext context);

        protected internal delegate Task<(long, object)> SetupFunc<in T>(TransactionOperationContext context, string databaseName, T json, string raftRequestId);

        protected internal delegate Task WaitForIndexFunc(TransactionOperationContext context, long index);

        protected internal async Task DatabaseConfigurations(SetupFunc<BlittableJsonReaderObject> setupConfigurationFunc,
            string debug,
            string raftRequestId,
            string databaseName,
            WaitForIndexFunc waitForIndex,
            RefAction<BlittableJsonReaderObject> beforeSetupConfiguration = null,
            Action<DynamicJsonValue, BlittableJsonReaderObject, long> fillJson = null,
            HttpStatusCode statusCode = HttpStatusCode.OK)
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var configurationJson = await context.ReadForMemoryAsync(RequestBodyStream(), debug);
                var result = await DatabaseConfigurations(setupConfigurationFunc, context, raftRequestId, databaseName, configurationJson, waitForIndex, beforeSetupConfiguration);

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

        private async Task<(long Index, T Configuration)> DatabaseConfigurations<T>(SetupFunc<T> setupConfigurationFunc, 
            TransactionOperationContext context, 
            string raftRequestId, 
            string databaseName, 
            T configurationJson,
            WaitForIndexFunc waitForIndex,
            RefAction<T> beforeSetupConfiguration = null)
        {
            if (await CanAccessDatabaseAsync(databaseName, requireAdmin: true, requireWrite: true) == false)
                return (-1, default);

            if (ResourceNameValidator.IsValidResourceName(databaseName, ServerStore.Configuration.Core.DataDirectory.FullPath, out string errorMessage) == false)
                throw new BadRequestException(errorMessage);

            await ServerStore.EnsureNotPassiveAsync();

            beforeSetupConfiguration?.Invoke(databaseName, ref configurationJson, context);

            var (index, _) = await setupConfigurationFunc(context, databaseName, configurationJson, raftRequestId);
            await waitForIndex(context, index);

            return (index, configurationJson);
        }

        protected async Task ResetEtl(string databaseName, WaitForIndexFunc waitForIndex)
        {
            var configurationName = GetStringQueryString("configurationName"); // etl task name
            var transformationName = GetStringQueryString("transformationName");

            await DatabaseConfigurations((_, db, etlConfiguration, guid) => ServerStore.RemoveEtlProcessState(_, db, configurationName, transformationName, guid),
                "etl-reset", GetRaftRequestIdFromQuery(), databaseName, waitForIndex);
        }

        protected async Task AddEtl(string databaseName, WaitForIndexFunc waitForIndex)
        {
            var id = GetLongQueryString("id", required: false);

            if (id == null)
            {
                await DatabaseConfigurations((_, db, etlConfiguration, guid) =>
                        ServerStore.AddEtl(_, db, etlConfiguration, guid), "etl-add",
                    GetRaftRequestIdFromQuery(),
                    databaseName,
                    waitForIndex,
                    beforeSetupConfiguration: AssertCanAddOrUpdateEtl,
                    fillJson: (json, _, index) => json[nameof(EtlConfiguration<ConnectionString>.TaskId)] = index);

                return;
            }

            string etlConfigurationName = null;

            await DatabaseConfigurations((_, db, etlConfiguration, guid) =>
            {
                var task = ServerStore.UpdateEtl(_, db, id.Value, etlConfiguration, guid);
                etlConfiguration.TryGet(nameof(RavenEtlConfiguration.Name), out etlConfigurationName);
                return task;
            }, "etl-update",
                GetRaftRequestIdFromQuery(),
                databaseName,
                waitForIndex,
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
                    await ServerStore.RemoveEtlProcessState(ctx, databaseName, etlConfigurationName, script, $"{raftRequestId}/{script}");
                }
            }
        }

        protected async Task ToggleTaskState(string databaseName, WaitForIndexFunc waitForIndex)
        {
            if (ResourceNameValidator.IsValidResourceName(databaseName, ServerStore.Configuration.Core.DataDirectory.FullPath, out string errorMessage) == false)
                throw new BadRequestException(errorMessage);

            var key = GetLongQueryString("key");
            var typeStr = GetQueryStringValueAndAssertIfSingleAndNotEmpty("type");
            var disable = GetBoolValueQueryString("disable") ?? true;
            var taskName = GetStringQueryString("taskName", required: false);

            if (Enum.TryParse<OngoingTaskType>(typeStr, true, out var type) == false)
                throw new ArgumentException($"Unknown task type: {type}", nameof(type));

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var (index, _) = await ServerStore.ToggleTaskState(key, taskName, type, disable, databaseName, GetRaftRequestIdFromQuery());
                await waitForIndex(context, index);

                HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, new DynamicJsonValue
                    {
                        [nameof(ModifyOngoingTaskResult.TaskId)] = key,
                        [nameof(ModifyOngoingTaskResult.RaftCommandIndex)] = index
                    });
                }
            }
        }

        protected async Task DeleteOngoingTask(string databaseName, DocumentDatabase database, WaitForIndexFunc waitForIndex)
        {
            if (ResourceNameValidator.IsValidResourceName(databaseName, ServerStore.Configuration.Core.DataDirectory.FullPath, out string errorMessage) == false)
                throw new BadRequestException(errorMessage);

            var id = GetLongQueryString("id");
            var typeStr = GetQueryStringValueAndAssertIfSingleAndNotEmpty("type");
            var taskName = GetStringQueryString("taskName", required: false);

            if (Enum.TryParse<OngoingTaskType>(typeStr, true, out var type) == false)
                throw new ArgumentException($"Unknown task type: {type}", "type");

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                long index;
                var action = new DeleteOngoingTaskAction(id, type, ServerStore, database, databaseName, context);
                var raftRequestId = GetRaftRequestIdFromQuery();

                try
                {
                    (index, _) = await ServerStore.DeleteOngoingTask(id, taskName, type, databaseName, $"{raftRequestId}/delete-ongoing-task");
                    await waitForIndex(context, index);

                    if (type == OngoingTaskType.Subscription)
                    {
                        database.SubscriptionStorage.RaiseNotificationForTaskRemoved(taskName);
                    }
                }
                finally
                {
                    await action.Complete($"{raftRequestId}/complete");
                }

                HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, new DynamicJsonValue
                    {
                        [nameof(ModifyOngoingTaskResult.TaskId)] = id,
                        [nameof(ModifyOngoingTaskResult.RaftCommandIndex)] = index
                    });
                }
            }
        }

        protected async Task UpdatePeriodicBackup(string databaseName, WaitForIndexFunc waitForIndex)
        {
            await DatabaseConfigurations(ServerStore.ModifyPeriodicBackup,
                "update-periodic-backup",
                GetRaftRequestIdFromQuery(),
                databaseName,
                waitForIndex,
                beforeSetupConfiguration: (string dbName, ref BlittableJsonReaderObject readerObject, JsonOperationContext context) =>
                {
                    var configuration = JsonDeserializationCluster.PeriodicBackupConfiguration(readerObject);

                    ServerStore.LicenseManager.AssertCanAddPeriodicBackup(configuration);
                    BackupConfigurationHelper.UpdateLocalPathIfNeeded(configuration, ServerStore);
                    BackupConfigurationHelper.AssertBackupConfiguration(configuration);
                    BackupConfigurationHelper.AssertDestinationAndRegionAreAllowed(configuration, ServerStore);

                    readerObject = context.ReadObject(configuration.ToJson(), "updated-backup-configuration");
                },
                fillJson: (json, readerObject, index) =>
                {
                    var taskIdName = nameof(PeriodicBackupConfiguration.TaskId);
                    readerObject.TryGet(taskIdName, out long taskId);
                    if (taskId == 0)
                        taskId = index;
                    json[taskIdName] = taskId;
                });
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
                case EtlType.ElasticSearch:
                    ServerStore.LicenseManager.AssertCanAddElasticSearchEtl();
                    break;
                default:
                    throw new NotSupportedException($"Unknown ETL configuration type. Configuration: {etlConfiguration}");
            }
        }

        private class DeleteOngoingTaskAction
        {
            private readonly ServerStore _serverStore;
            private readonly DocumentDatabase _database;
            private readonly TransactionOperationContext _context;
            private readonly (string Name, List<string> Transformations) _deletingEtl;
            private readonly string _databaseName;

            public DeleteOngoingTaskAction(long id, OngoingTaskType type, ServerStore serverStore, DocumentDatabase database, string databaseName, TransactionOperationContext context)
            {
                _serverStore = serverStore;
                _database = database;
                _context = context;
                _databaseName = databaseName;

                switch (type)
                {
                    case OngoingTaskType.RavenEtl:
                    case OngoingTaskType.SqlEtl:
                        using (context.Transaction == null ? context.OpenReadTransaction() : null)
                        using (var rawRecord = _serverStore.Cluster.ReadRawDatabaseRecord(context, _databaseName))
                        {
                            if (rawRecord == null)
                                break;

                            if (type == OngoingTaskType.RavenEtl)
                            {
                                var ravenEtls = rawRecord.RavenEtls;
                                var ravenEtl = ravenEtls?.Find(x => x.TaskId == id);
                                if (ravenEtl != null)
                                    _deletingEtl = (ravenEtl.Name, ravenEtl.Transforms.Where(x => string.IsNullOrEmpty(x.Name) == false).Select(x => x.Name).ToList());
                            }
                            else
                            {
                                var sqlEtls = rawRecord.SqlEtls;
                                var sqlEtl = sqlEtls?.Find(x => x.TaskId == id);
                                if (sqlEtl != null)
                                    _deletingEtl = (sqlEtl.Name, sqlEtl.Transforms.Where(x => string.IsNullOrEmpty(x.Name) == false).Select(x => x.Name).ToList());
                            }
                        }
                        break;
                }
            }

            public async Task Complete(string raftRequestId)
            {
                if (_deletingEtl.Name != null)
                {
                    foreach (var transformation in _deletingEtl.Transformations)
                    {
                        var (index, _) = await _serverStore.RemoveEtlProcessState(_context, _databaseName, _deletingEtl.Name, transformation,
                            $"{raftRequestId}/{transformation}");
                        await _database.RachisLogIndexNotifications.WaitForIndexNotification(index, _serverStore.Engine.OperationTimeout);
                    }
                }
            }
        }
    }
}

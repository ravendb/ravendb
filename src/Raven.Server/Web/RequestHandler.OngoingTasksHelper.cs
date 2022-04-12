using System;
using System.Net;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Exceptions;
using Raven.Client.Util;
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
    }
}

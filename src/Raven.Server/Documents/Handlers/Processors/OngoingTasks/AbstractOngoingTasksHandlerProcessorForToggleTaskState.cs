using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Server.Documents.Handlers.Processors.Databases;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.ServerWide.Commands.Subscriptions;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Processors.OngoingTasks
{
    internal abstract class AbstractOngoingTasksHandlerProcessorForToggleTaskState<TRequestHandler, TOperationContext> : AbstractHandlerProcessorForUpdateDatabaseTask<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        private long _key;
        private string _desc;

        protected AbstractOngoingTasksHandlerProcessorForToggleTaskState([NotNull] TRequestHandler requestHandler)
            : base(requestHandler)
        {
        }

        protected override void OnBeforeResponseWrite(TransactionOperationContext context, DynamicJsonValue responseJson, object _, long index)
        {
            responseJson[nameof(ModifyOngoingTaskResult.TaskId)] = _key;
        }

        protected override ValueTask OnAfterUpdateConfiguration(TransactionOperationContext context, object configuration, string raftRequestId)
        {
            RequestHandler.LogTaskToAudit(_desc, _key, configuration: null);
            return ValueTask.CompletedTask;
        }

        protected override Task<(long Index, object Result)> OnUpdateConfiguration(TransactionOperationContext context, object _, string raftRequestId)
        {
            _key = RequestHandler.GetLongQueryString("key");

            var typeStr = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("type");
            var disable = RequestHandler.GetBoolValueQueryString("disable") ?? true;
            var taskName = RequestHandler.GetStringQueryString("taskName", required: false);

            if (Enum.TryParse<OngoingTaskType>(typeStr, true, out var type) == false)
                throw new ArgumentException($"Unknown task type: {type}", nameof(type));

            _desc = (disable) ? "disable" : "enable";
            _desc += $"-{typeStr}-Task {(string.IsNullOrEmpty(taskName) ? string.Empty : $" with task name: '{taskName}'")}";

            return ToggleTaskState(_key, taskName, type, disable, RequestHandler.DatabaseName, raftRequestId);
        }

        private async Task<(long Index, object Result)> ToggleTaskState(long taskId, string taskName, OngoingTaskType type, bool disable, string dbName, string raftRequestId)
        {
            CommandBase disableEnableCommand;
            switch (type)
            {
                case OngoingTaskType.Subscription:
                    if (taskName == null)
                    {
                        using (RequestHandler.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext ctx))
                        using (ctx.OpenReadTransaction())
                        {
                            taskName = GetSubscriptionStorage().GetSubscriptionNameById(ctx, taskId);
                        }
                    }
                    disableEnableCommand = new ToggleSubscriptionStateCommand(taskName, disable, dbName, raftRequestId);
                    break;

                default:
                    disableEnableCommand = new ToggleTaskStateCommand(taskId, type, disable, dbName, raftRequestId);
                    break;
            }
            return await RequestHandler.ServerStore.SendToLeaderAsync(disableEnableCommand);
        }

        protected abstract AbstractSubscriptionStorage GetSubscriptionStorage();
    }
}

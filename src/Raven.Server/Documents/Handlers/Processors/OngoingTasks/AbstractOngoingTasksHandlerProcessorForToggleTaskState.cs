using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Server.Documents.Handlers.Processors.Databases;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Processors.OngoingTasks
{
    internal abstract class AbstractOngoingTasksHandlerProcessorForToggleTaskState<TRequestHandler> : AbstractHandlerProcessorForUpdateDatabaseConfiguration<BlittableJsonReaderObject, TRequestHandler>
        where TRequestHandler : RequestHandler
    {
        private long _key;

        protected AbstractOngoingTasksHandlerProcessorForToggleTaskState([NotNull] TRequestHandler requestHandler)
            : base(requestHandler)
        {
        }

        protected override void OnBeforeResponseWrite(DynamicJsonValue responseJson, BlittableJsonReaderObject configuration, long index)
        {
            responseJson[nameof(ModifyOngoingTaskResult.TaskId)] = _key;
        }

        protected override Task<(long Index, object Result)> OnUpdateConfiguration(TransactionOperationContext context, string databaseName, BlittableJsonReaderObject configuration, string raftRequestId)
        {
            _key = RequestHandler.GetLongQueryString("key");

            var typeStr = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("type");
            var disable = RequestHandler.GetBoolValueQueryString("disable") ?? true;
            var taskName = RequestHandler.GetStringQueryString("taskName", required: false);

            if (Enum.TryParse<OngoingTaskType>(typeStr, true, out var type) == false)
                throw new ArgumentException($"Unknown task type: {type}", nameof(type));

            return RequestHandler.ServerStore.ToggleTaskState(_key, taskName, type, disable, databaseName, raftRequestId);
        }
    }
}

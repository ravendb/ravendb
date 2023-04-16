using System;
using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Exceptions.Database;
using Raven.Client.Http;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Documents.OngoingTasks;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;

namespace Raven.Server.Web.System.Processors.OngoingTasks;

internal abstract class AbstractOngoingTasksHandlerProcessorForGetOngoingTask<TRequestHandler, TOperationContext, TSubscriptionConnectionsState> : AbstractHandlerProxyReadProcessor<OngoingTask, TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    where TSubscriptionConnectionsState : AbstractSubscriptionConnectionsState
{
    private readonly AbstractOngoingTasks<TSubscriptionConnectionsState> _ongoingTasks;

    protected AbstractOngoingTasksHandlerProcessorForGetOngoingTask([NotNull] TRequestHandler requestHandler, [NotNull] AbstractOngoingTasks<TSubscriptionConnectionsState> ongoingTasks)
        : base(requestHandler)
    {
        _ongoingTasks = ongoingTasks ?? throw new ArgumentNullException(nameof(ongoingTasks));
    }

    protected override bool SupportsCurrentNode => true;

    protected long? GetTaskId() => RequestHandler.GetLongQueryString("key", required: false);

    protected string GetTaskName(long? taskId) => RequestHandler.GetStringQueryString("taskName", required: taskId == null);

    protected OngoingTaskType GetTaskType()
    {
        var typeStr = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("type");

        if (Enum.TryParse<OngoingTaskType>(typeStr, true, out var type) == false)
            throw new ArgumentException($"Unknown task type '{type}'.");

        return type;
    }

    protected override async ValueTask HandleCurrentNodeAsync()
    {
        var taskId = GetTaskId();
        var taskName = GetTaskName(taskId);
        var taskType = GetTaskType();

        using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        using (context.OpenReadTransaction())
        {
            var clusterTopology = ServerStore.GetClusterTopology(context);
            var record = ServerStore.Cluster.ReadDatabase(context, RequestHandler.DatabaseName);
            if (record == null)
                throw new DatabaseDoesNotExistException(RequestHandler.DatabaseName);

            var result = _ongoingTasks.GetTask(context, taskId, taskName, taskType, clusterTopology, record);

            if (result == null)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                return;
            }

            await using (var writer = new AsyncBlittableJsonTextWriterForDebug(context, ServerStore, RequestHandler.ResponseBodyStream()))
            {
                context.Write(writer, result.ToJson());
            }
        }
    }

    protected override RavenCommand<OngoingTask> CreateCommandForNode(string nodeTag)
    {
        var taskId = GetTaskId();
        var taskType = GetTaskType();

        if (taskId.HasValue)
            return new GetOngoingTaskInfoOperation.GetOngoingTaskInfoCommand(taskId.Value, taskType);

        var taskName = GetTaskName(taskId: null);
        return new GetOngoingTaskInfoOperation.GetOngoingTaskInfoCommand(taskName, taskType);
    }
}

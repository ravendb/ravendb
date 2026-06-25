using System.Diagnostics.CodeAnalysis;
using System.Threading.Tasks;
using Raven.Server.Documents.ETL;
using Raven.Server.Documents.ETL.Handlers.Processors;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.Sharding;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;

namespace Raven.Server.Documents.CdcSink.Handlers.Processors;

internal sealed class CdcSinkHandlerProcessorForGetErrors : AbstractTaskErrorsHandlerProcessorForGetErrors<DatabaseRequestHandler, DocumentsOperationContext>
{
    public CdcSinkHandlerProcessorForGetErrors([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override TaskCategory TaskCategory => TaskCategory.CdcSink;

    protected override bool SupportsCurrentNode => true;

    protected override async ValueTask HandleCurrentNodeAsync()
    {
        var response = new TaskErrorsResponse
        {
            NodeTag = RequestHandler.ServerStore.NodeTag,
            ShardNumber = RequestHandler.Database is ShardedDocumentDatabase shardedDatabase ? shardedDatabase.ShardNumber : null
        };

        var storage = RequestHandler.Database.TaskErrorsStorage;
        var taskNames = GetNames();

        var errorsByTask = taskNames.Count == 0
            ? storage.ReadAllErrorsGroupedByTask(TaskCategory)
            : storage.ReadErrorsForTasks(TaskCategory, taskNames);

        // CDC sinks are not ETL processes, so there is no EtlType/EtlSubType to enrich with.
        foreach (var (taskName, processErrors, itemErrors) in errorsByTask)
            response.Results.Add(BuildTaskErrors(taskName, process: null, processErrors, itemErrors, TaskCategory));

        await WriteTaskErrorsResponseAsync(response, "cdc-sink/errors");
    }

    protected override Task HandleRemoteNodeAsync(ProxyCommand<TaskErrors[]> command, OperationCancelToken token) => RequestHandler.ExecuteRemoteAsync(command, token.Token);
}

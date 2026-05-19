using System;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading.Tasks;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.Sharding;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;

namespace Raven.Server.Documents.ETL.Handlers.Processors;

internal sealed class EtlHandlerProcessorForGetErrors : AbstractTaskErrorsHandlerProcessorForGetErrors<DatabaseRequestHandler, DocumentsOperationContext>
{
    public EtlHandlerProcessorForGetErrors([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override TaskCategory TaskCategory => TaskCategory.Etl;

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
        var processesByName = RequestHandler.Database.EtlLoader.Processes
            .ToDictionary(p => p.Name, p => p, StringComparer.Ordinal);

        var errorsByTask = taskNames.Count == 0
            ? storage.ReadAllErrorsGroupedByTask(TaskCategory)
            : storage.ReadErrorsForTasks(TaskCategory, taskNames);

        foreach (var (taskName, processErrors, itemErrors) in errorsByTask)
        {
            processesByName.TryGetValue(taskName, out var process);
            response.Results.Add(BuildTaskErrors(taskName, process, processErrors, itemErrors));
        }

        await WriteTaskErrorsResponseAsync(response, "etl/errors");
    }

    protected override Task HandleRemoteNodeAsync(ProxyCommand<TaskErrors[]> command, OperationCancelToken token) => RequestHandler.ExecuteRemoteAsync(command, token.Token);
}

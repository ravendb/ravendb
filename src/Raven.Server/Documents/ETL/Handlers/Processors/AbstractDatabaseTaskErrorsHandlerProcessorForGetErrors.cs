using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading.Tasks;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.Sharding;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;
using Raven.Server.Documents.TasksErrors;

namespace Raven.Server.Documents.ETL.Handlers.Processors;

// Shared current-node/proxy body for the per-category "get task errors" endpoints (ETL, AI, CDC Sink)
// on a non-sharded database. Subclasses supply the task category, the endpoint's debug name, and
// (optionally) how to resolve the live process used to enrich each result.
internal abstract class AbstractDatabaseTaskErrorsHandlerProcessorForGetErrors : AbstractTaskErrorsHandlerProcessorForGetErrors<DatabaseRequestHandler, DocumentsOperationContext>
{
    protected AbstractDatabaseTaskErrorsHandlerProcessorForGetErrors([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected abstract string EndpointDebugName { get; }

    // The live ETL process supplies each result's EtlType and EtlSubType (see BuildTaskErrors). ETL and AI
    // tasks are ETL processes, so they resolve it by name here; CDC Sink is not an ETL process and overrides
    // this to return null, leaving EtlType/EtlSubType unset on its results.
    protected virtual IReadOnlyDictionary<string, EtlProcess> GetProcessesByName() =>
        RequestHandler.Database.EtlLoader.Processes.ToDictionary(p => p.Name, p => p, StringComparer.Ordinal);

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
        var processesByName = GetProcessesByName();

        var errorsByTask = taskNames.Count == 0
            ? storage.ReadAllErrorsGroupedByTask(TaskCategory)
            : storage.ReadErrorsForTasks(TaskCategory, taskNames);

        foreach (var (taskName, processErrors, itemErrors) in errorsByTask)
        {
            EtlProcess process = null;
            processesByName?.TryGetValue(taskName, out process);
            response.Results.Add(BuildTaskErrors(taskName, process, processErrors, itemErrors, TaskCategory));
        }

        await WriteTaskErrorsResponseAsync(response, EndpointDebugName);
    }

    protected override Task HandleRemoteNodeAsync(ProxyCommand<TaskErrors[]> command, OperationCancelToken token) => RequestHandler.ExecuteRemoteAsync(command, token.Token);
}

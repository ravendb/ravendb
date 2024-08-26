using System;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes;
using Raven.Server.Json;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Web.Http;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.Documents.Handlers.Processors.Indexes
{
    internal sealed class IndexHandlerProcessorForGetDatabaseIndexStatistics : AbstractIndexHandlerProcessorForGetDatabaseIndexStatistics<DatabaseRequestHandler,
         DocumentsOperationContext>
    {
        public IndexHandlerProcessorForGetDatabaseIndexStatistics([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }
        protected override bool SupportsCurrentNode => true;

        protected override ValueTask HandleCurrentNodeAsync()
        {
            var name = RequestHandler.GetStringQueryString("name", required: false);

            using (var context = QueryOperationContext.Allocate(RequestHandler.Database, needsServerContext: true))
            {
                IndexStats[] indexesStats;
                using (context.OpenReadTransaction())
                {
                    if (string.IsNullOrEmpty(name))
                    {
                        indexesStats = RequestHandler.Database.IndexStore
                            .GetIndexes()
                            .OrderBy(x => x.Name)
                            .Select(x =>
                            {
                                try
                                {
                                    return x.GetStats(calculateLag: true, calculateStaleness: true, calculateMemoryStats: ShouldCalculateStats, queryContext: context);
                                }
                                catch (OperationCanceledException)
                                {
                                    // we probably closed the indexing thread
                                    return new IndexStats
                                    {
                                        Name = x.Name,
                                        Type = x.Type,
                                        State = x.State,
                                        Status = x.Status,
                                        LockMode = x.Definition.LockMode,
                                        ArchivedDataProcessingBehavior = x.Definition.ArchivedDataProcessingBehavior,
                                        Priority = x.Definition.Priority,
                                        ReferencedCollections = x.GetReferencedCollectionNames()
                                    };
                                }
                                catch (Exception e)
                                {
                                    if (Logger.IsErrorEnabled)
                                        Logger.Error($"Failed to get stats of '{x.Name}' index", e);

                                    try
                                    {
                                        RequestHandler.Database.NotificationCenter.Add(AlertRaised.Create(RequestHandler.Database.Name,
                                            $"Failed to get stats of '{x.Name}' index",
                                            $"Exception was thrown on getting stats of '{x.Name}' index",
                                            AlertType.Indexing_CouldNotGetStats, NotificationSeverity.Error, key: x.Name, details: new ExceptionDetails(e)));
                                    }
                                    catch (Exception addAlertException)
                                    {
                                        if (Logger.IsWarnEnabled && addAlertException.IsOutOfMemory() == false &&
                                            addAlertException.IsRavenDiskFullException() == false)
                                            Logger.Warn($"Failed to add alert when getting error on retrieving stats of '{x.Name}' index", addAlertException);
                                    }

                                    var state = x.State;

                                    if (e.IsOutOfMemory() == false &&
                                        e.IsRavenDiskFullException() == false)
                                    {
                                        try
                                        {
                                            state = IndexState.Error;
                                            x.SetState(state, inMemoryOnly: true);
                                        }
                                        catch (Exception ex)
                                        {
                                            if (Logger.IsWarnEnabled)
                                                Logger.Warn(
                                                    $"Failed to change state of '{x.Name}' index to error after encountering exception when getting its stats.",
                                                    ex);
                                        }
                                    }

                                    return new IndexStats
                                    {
                                        Name = x.Name,
                                        Type = x.Type,
                                        State = state,
                                        Status = x.Status,
                                        LockMode = x.Definition.LockMode,
                                        ArchivedDataProcessingBehavior = x.Definition.ArchivedDataProcessingBehavior,
                                        Priority = x.Definition.Priority,
                                        ReferencedCollections = x.GetReferencedCollectionNames()
                                    };
                                }
                            })
                            .ToArray();
                    }
                    else
                    {
                        var index = RequestHandler.Database.IndexStore.GetIndex(name);
                        if (index == null)
                        {
                            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                            return ValueTask.CompletedTask;
                        }

                        indexesStats = new[] { index.GetStats(calculateLag: true, calculateStaleness: true, calculateMemoryStats: ShouldCalculateStats, queryContext: context) };
                    }
                }

                return WriteResultAsync(indexesStats);
            }
        }

        protected override Task HandleRemoteNodeAsync(ProxyCommand<IndexStats[]> command, OperationCancelToken token) => RequestHandler.ExecuteRemoteAsync(command, token.Token);

        private async ValueTask WriteResultAsync(IndexStats[] result)
        {
            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriterForDebug(context, ServerStore, RequestHandler.ResponseBodyStream()))
                writer.WriteIndexesStats(context, result);
        }
    }
}

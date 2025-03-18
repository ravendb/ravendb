using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Indexes;

internal sealed class IndexHandlerProcessorForProgress : AbstractIndexHandlerProcessorForProgress<DatabaseRequestHandler, DocumentsOperationContext>
{
    public IndexHandlerProcessorForProgress([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override bool SupportsCurrentNode => true;

    protected override ValueTask HandleCurrentNodeAsync()
    {
        var indexesProgress = GetIndexesProgress().ToArray();

        return WriteResultAsync(indexesProgress);
    }

    protected override Task HandleRemoteNodeAsync(ProxyCommand<IndexProgress[]> command, OperationCancelToken token) => RequestHandler.ExecuteRemoteAsync(command, token.Token);

    private IEnumerable<IndexProgress> GetIndexesProgress()
    {
        using (var context = QueryOperationContext.Allocate(RequestHandler.Database, needsServerContext: true))
        using (context.OpenReadTransaction())
        {
            var overallDuration = Stopwatch.StartNew();

            foreach (var index in RequestHandler.Database.IndexStore.GetIndexes())
            {
                IndexProgress indexProgress = null;
                try
                {
                    if (index.DeployedOnAllNodes && index.IsStale(context) == false)
                        continue;

                    indexProgress = index.GetProgress(context, overallDuration);
                }
                catch (ObjectDisposedException)
                {
                    // index was deleted
                }
                catch (OperationCanceledException)
                {
                    // index was deleted
                }
                catch (Exception e)
                {
                    if (Logger.IsErrorEnabled)
                        Logger.Error($"Failed to get index progress for index name: {index.Name}", e);
                }

                if (indexProgress == null)
                    continue;

                yield return indexProgress;
            }
        }
    }

    private async ValueTask WriteResultAsync(IndexProgress[] result)
    {
        using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
        await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
        {
            writer.WriteStartObject();

            writer.WriteArray(context, "Results", result, (w, c, progress) => w.WriteIndexProgress(c, progress));

            writer.WriteEndObject();
        }
    }
}

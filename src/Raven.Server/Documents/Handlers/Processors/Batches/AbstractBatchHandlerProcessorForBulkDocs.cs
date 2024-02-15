using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Json;
using Raven.Server.Documents.Handlers.Batches;
using Raven.Server.Documents.Handlers.Batches.Commands;
using Raven.Server.TrafficWatch;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Processors.Batches;

internal abstract class AbstractBatchHandlerProcessorForBulkDocs<TBatchCommand, TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
    where TBatchCommand : class, IBatchCommand
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractBatchHandlerProcessorForBulkDocs([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected abstract ValueTask<DynamicJsonArray> HandleTransactionAsync(JsonOperationContext context, TBatchCommand command, IndexBatchOptions indexBatchOptions, ReplicationBatchOptions replicationBatchOptions);

    protected abstract ValueTask WaitForIndexesAsync(IndexBatchOptions options, string lastChangeVector, long lastTombstoneEtag, HashSet<string> modifiedCollections, CancellationToken token = default);

    protected abstract ValueTask WaitForReplicationAsync(TOperationContext context, ReplicationBatchOptions options, string lastChangeVector);

    protected abstract char GetIdentityPartsSeparator();

    protected abstract AbstractBatchCommandsReader<TBatchCommand, TOperationContext> GetCommandsReader();

    protected abstract AbstractClusterTransactionRequestProcessor<TRequestHandler, TBatchCommand> GetClusterTransactionRequestProcessor();

    public override async ValueTask ExecuteAsync()
    {
        try
        {
            await ExecuteInternalAsync();
        }
        catch
        {
            // TODO: this logic should probably be global for any request
            if (RequestHandler.IsShutdownRequested())
                RequestHandler.ThrowShutdownException();

            throw;
        }
    }

    public async ValueTask ExecuteInternalAsync()
    {
        var indexBatchOptions = GetIndexBatchOptions();
        var replicationBatchOptions = GetReplicationBatchOptions();

        using (var commandsReader = GetCommandsReader())
        using (ContextPool.AllocateOperationContext(out TOperationContext context))
        using (var token = RequestHandler.CreateHttpRequestBoundOperationToken())
        {
            var contentType = HttpContext.Request.ContentType;
            try
            {
                if (contentType == null ||
                    contentType.StartsWith("application/json", StringComparison.OrdinalIgnoreCase))
                {
                    await commandsReader.BuildCommandsAsync(context, RequestHandler.RequestBodyStream(), GetIdentityPartsSeparator(), token.Token).ConfigureAwait(false);
                }
                else if (contentType.StartsWith("multipart/mixed", StringComparison.OrdinalIgnoreCase) ||
                         contentType.StartsWith("multipart/form-data", StringComparison.OrdinalIgnoreCase))
                {
                    await commandsReader.ParseMultipart(context, RequestHandler.RequestBodyStream(), HttpContext.Request.ContentType, GetIdentityPartsSeparator(), token.Token).ConfigureAwait(false);
                }
                else
                    ThrowNotSupportedType(contentType);
            }
            finally
            {
                if (TrafficWatchManager.HasRegisteredClients)
                {
                    var log = BatchTrafficWatch(commandsReader.Commands);
                    // add sb to httpContext
                    RequestHandler.AddStringToHttpContext(log, TrafficWatchChangeType.BulkDocs);
                }
            }

            using (var command = await commandsReader.GetCommandAsync(context).ConfigureAwait(false))
            {
                if (command.IsClusterTransaction)
                {
                    var processor = GetClusterTransactionRequestProcessor();
                    (long index, DynamicJsonArray clusterResults) = await processor.ProcessAsync(context, command, token.Token)
                                                                                   .ConfigureAwait(false);

                    RequestHandler.HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;

                    await using (AsyncBlittableJsonTextWriter.Create(context, RequestHandler.ResponseBodyStream(), out var writer))
                    {
                        context.Write(writer, new DynamicJsonValue
                        {
                            [nameof(BatchCommandResult.Results)] = clusterResults,
                            [nameof(BatchCommandResult.TransactionIndex)] = index
                        });
                    }

                    return;
                }

                if (indexBatchOptions != null)
                    command.ModifiedCollections = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

                var noReply = RequestHandler.GetBoolValueQueryString("noreply", required: false);
                if (noReply.HasValue)
                    command.IncludeReply = noReply.Value == false;

                var results = await HandleTransactionAsync(context, command, indexBatchOptions, replicationBatchOptions).ConfigureAwait(false);

                if (replicationBatchOptions != null)
                {
                    await WaitForReplicationAsync(context, replicationBatchOptions, command.LastChangeVector).ConfigureAwait(false);
                }

                if (indexBatchOptions != null)
                {
                    await WaitForIndexesAsync(indexBatchOptions, command.LastChangeVector, command.LastTombstoneEtag, command.ModifiedCollections).ConfigureAwait(false);
                }

                HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;

                await using (AsyncBlittableJsonTextWriter.Create(context, RequestHandler.ResponseBodyStream(), out var writer))
                {
                    context.Write(writer, new DynamicJsonValue
                    {
                        [nameof(BatchCommandResult.Results)] = results
                    });
                }
            }
        }
    }

    private IndexBatchOptions GetIndexBatchOptions()
    {
        var waitForIndexesTimeout = RequestHandler.GetTimeSpanQueryString("waitForIndexesTimeout", required: false);
        if (waitForIndexesTimeout == null)
            return null;

        return new IndexBatchOptions
        {
            WaitForIndexes = true,
            WaitForIndexesTimeout = waitForIndexesTimeout.Value,
            ThrowOnTimeoutInWaitForIndexes = RequestHandler.GetBoolValueQueryString("waitForIndexThrow", required: false) ?? true,
            WaitForSpecificIndexes = RequestHandler.GetStringValuesQueryString("waitForSpecificIndex", required: false)
        };
    }

    private ReplicationBatchOptions GetReplicationBatchOptions()
    {
        var waitForReplicasTimeout = RequestHandler.GetTimeSpanQueryString("waitForReplicasTimeout", required: false);
        if (waitForReplicasTimeout == null)
            return null;

        var numberOfReplicasStr = RequestHandler.GetStringQueryString("numberOfReplicasToWaitFor", required: false);
        var numberOfReplicas = 1;
        var majority = numberOfReplicasStr == "majority";
        if (majority == false)
        {
            if (int.TryParse(numberOfReplicasStr, out numberOfReplicas) == false)
                RequestHandler.ThrowInvalidInteger("numberOfReplicasToWaitFor", numberOfReplicasStr);
        }

        return new ReplicationBatchOptions
        {
            WaitForReplicas = true,
            Majority = majority,
            NumberOfReplicasToWaitFor = numberOfReplicas,
            ThrowOnTimeoutInWaitForReplicas = RequestHandler.GetBoolValueQueryString("throwOnTimeoutInWaitForReplicas", required: false) ?? true,
            WaitForReplicasTimeout = waitForReplicasTimeout.Value
        };
    }

    private static string BatchTrafficWatch(ArraySegment<BatchRequestParser.CommandData> parsedCommands)
    {
        var sb = new StringBuilder();
        for (var i = parsedCommands.Offset; i < (parsedCommands.Offset + parsedCommands.Count); i++)
        {
            // log script and args if type is patch
            if (parsedCommands.Array[i].Type == CommandType.PATCH)
            {
                sb.Append(parsedCommands.Array[i].Type).Append("    ")
                    .Append(parsedCommands.Array[i].Id).Append("    ")
                    .Append(parsedCommands.Array[i].Patch.Script).Append("    ")
                    .Append(parsedCommands.Array[i].PatchArgs).AppendLine();
            }
            else
            {
                sb.Append(parsedCommands.Array[i].Type).Append("    ")
                    .Append(parsedCommands.Array[i].Id).AppendLine();
            }
        }

        return sb.ToString();
    }

    [DoesNotReturn]
    private static void ThrowNotSupportedType(string contentType)
    {
        throw new InvalidOperationException($"The requested Content type '{contentType}' is not supported. Use 'application/json' or 'multipart/mixed'.");
    }
}

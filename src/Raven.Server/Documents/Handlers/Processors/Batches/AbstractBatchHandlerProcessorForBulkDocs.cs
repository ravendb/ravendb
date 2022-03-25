using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Json;
using Raven.Server.TrafficWatch;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Processors.Batches;

internal abstract class AbstractBatchHandlerProcessorForBulkDocs<TBatchCommand, TRequestHandler, TOperationContext> : AbstractHandlerProcessor<TRequestHandler, TOperationContext>
    where TBatchCommand : BatchHandler.IBatchCommand
    where TRequestHandler : RequestHandler
    where TOperationContext : JsonOperationContext
{
    protected AbstractBatchHandlerProcessorForBulkDocs([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> contextPool)
        : base(requestHandler, contextPool)
    {
    }

    protected abstract ValueTask<DynamicJsonArray> HandleTransactionAsync(TBatchCommand command);

    protected abstract ValueTask WaitForIndexesAsync(TimeSpan timeout, List<string> specifiedIndexesQueryString, bool throwOnTimeout, string lastChangeVector, long lastTombstoneEtag, HashSet<string> modifiedCollections);

    protected abstract ValueTask WaitForReplicationAsync(TimeSpan waitForReplicasTimeout, string numberOfReplicasStr, bool throwOnTimeoutInWaitForReplicas, string lastChangeVector);

    protected abstract char GetIdentityPartsSeparator();

    protected abstract BatchRequestParser.AbstractBatchCommandBuilder<TBatchCommand, TOperationContext> GetCommandBuilder();

    protected abstract AbstractClusterTransactionRequestProcessor<TBatchCommand> GetClusterTransactionRequestProcessor();

    public override async ValueTask ExecuteAsync()
    {
        var waitForIndexesTimeout = RequestHandler.GetTimeSpanQueryString("waitForIndexesTimeout", required: false);
        var waitForIndexThrow = RequestHandler.GetBoolValueQueryString("waitForIndexThrow", required: false) ?? true;
        var specifiedIndexesQueryString = HttpContext.Request.Query["waitForSpecificIndex"];

        var commandBuilder = GetCommandBuilder();
        using (ContextPool.AllocateOperationContext(out TOperationContext context))
        {
            var contentType = HttpContext.Request.ContentType;
            try
            {
                if (contentType == null ||
                    contentType.StartsWith("application/json", StringComparison.OrdinalIgnoreCase))
                {
                    await commandBuilder.BuildCommandsAsync(context, RequestHandler.RequestBodyStream(), GetIdentityPartsSeparator());
                }
                else if (contentType.StartsWith("multipart/mixed", StringComparison.OrdinalIgnoreCase) ||
                         contentType.StartsWith("multipart/form-data", StringComparison.OrdinalIgnoreCase))
                {
                    await commandBuilder.ParseMultipart(context, RequestHandler.RequestBodyStream(), HttpContext.Request.ContentType, GetIdentityPartsSeparator());
                }
                else
                    ThrowNotSupportedType(contentType);
            }
            finally
            {
                if (TrafficWatchManager.HasRegisteredClients)
                {
                    var log = BatchTrafficWatch(commandBuilder.Commands);
                    // add sb to httpContext
                    RequestHandler.AddStringToHttpContext(log, TrafficWatchChangeType.BulkDocs);
                }
            }

            using (var command = await commandBuilder.GetCommandAsync(context))
            {
                if (command.IsClusterTransaction)
                {
                    var processor = GetClusterTransactionRequestProcessor();
                    (long index, DynamicJsonArray clusterResults) = await processor.ProcessAsync(context, command);

                    RequestHandler.HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;

                    await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                    {
                        context.Write(writer, new DynamicJsonValue
                        {
                            [nameof(BatchCommandResult.Results)] = clusterResults,
                            [nameof(BatchCommandResult.TransactionIndex)] = index
                        });
                    }

                    return;
                }

                if (waitForIndexesTimeout != null)
                    command.ModifiedCollections = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

                var results = await HandleTransactionAsync(command);

                var waitForReplicasTimeout = RequestHandler.GetTimeSpanQueryString("waitForReplicasTimeout", required: false);
                if (waitForReplicasTimeout != null)
                {
                    var numberOfReplicasStr = RequestHandler.GetStringQueryString("numberOfReplicasToWaitFor", required: false) ?? "1";
                    var throwOnTimeoutInWaitForReplicas = RequestHandler.GetBoolValueQueryString("throwOnTimeoutInWaitForReplicas", required: false) ?? true;

                    await WaitForReplicationAsync(waitForReplicasTimeout.Value, numberOfReplicasStr, throwOnTimeoutInWaitForReplicas, command.LastChangeVector);
                }

                if (waitForIndexesTimeout != null)
                {
                    await WaitForIndexesAsync(waitForIndexesTimeout.Value, specifiedIndexesQueryString.ToList(), waitForIndexThrow, command.LastChangeVector, command.LastTombstoneEtag, command.ModifiedCollections);
                }

                HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;

                await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                {
                    context.Write(writer, new DynamicJsonValue
                    {
                        [nameof(BatchCommandResult.Results)] = results
                    });
                }
            }
        }
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

    private static void ThrowNotSupportedType(string contentType)
    {
        throw new InvalidOperationException($"The requested Content type '{contentType}' is not supported. Use 'application/json' or 'multipart/mixed'.");
    }
}

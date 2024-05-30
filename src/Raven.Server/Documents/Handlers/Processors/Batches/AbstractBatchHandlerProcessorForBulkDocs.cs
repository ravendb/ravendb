using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.WebUtilities;
using Microsoft.Extensions.Primitives;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Json;
using Raven.Server.Documents.Handlers.Batches;
using Raven.Server.Documents.Handlers.Batches.Commands;
using Raven.Server.TrafficWatch;
using Raven.Server.Web;
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
        var parameters = QueryStringParameters.Create(HttpContext.Request);
        var indexBatchOptions = GetIndexBatchOptions(parameters);
        var replicationBatchOptions = GetReplicationBatchOptions(parameters);

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
                    var result = await processor.ProcessAsync(context, command, token.Token).ConfigureAwait(false);

                    RequestHandler.HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;

                    await using (AsyncBlittableJsonTextWriter.Create(context, RequestHandler.ResponseBodyStream(), out var writer))
                    {
                        context.Write(writer, new DynamicJsonValue
                        {
                            [nameof(BatchCommandResult.Results)] = result.Results,
                            [nameof(BatchCommandResult.TransactionIndex)] = result.Index
                        });
                    }

                    return;
                }

                if (indexBatchOptions != null)
                    command.ModifiedCollections = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

                if (parameters.NoReply.HasValue)
                    command.IncludeReply = parameters.NoReply.Value == false;

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

    private static IndexBatchOptions GetIndexBatchOptions(QueryStringParameters parameters)
    {
        var waitForIndexesTimeout = parameters.WaitForIndexesTimeout;
        if (waitForIndexesTimeout == null)
            return null;

        return new IndexBatchOptions
        {
            WaitForIndexes = true,
            WaitForIndexesTimeout = waitForIndexesTimeout.Value,
            ThrowOnTimeoutInWaitForIndexes = parameters.WaitForIndexThrow,
            WaitForSpecificIndexes = parameters.WaitForSpecificIndexes
        };
    }

    private static ReplicationBatchOptions GetReplicationBatchOptions(QueryStringParameters parameters)
    {
        var waitForReplicasTimeout = parameters.WaitForReplicasTimeout;
        if (waitForReplicasTimeout == null)
            return null;

        return new ReplicationBatchOptions
        {
            WaitForReplicas = true,
            Majority = parameters.Majority,
            NumberOfReplicasToWaitFor = parameters.NumberOfReplicasToWaitFor,
            ThrowOnTimeoutInWaitForReplicas = parameters.ThrowOnTimeoutInWaitForReplicas,
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

    private sealed class QueryStringParameters : AbstractQueryStringParameters
    {
        private static readonly ReadOnlyMemory<char> MajorityValue = "majority".AsMemory();

        public bool? NoReply;

        public TimeSpan? WaitForIndexesTimeout;

        public bool WaitForIndexThrow = true;

        public StringValues WaitForSpecificIndexes;

        public TimeSpan? WaitForReplicasTimeout;

        public bool Majority;

        public int NumberOfReplicasToWaitFor = 1;

        public bool ThrowOnTimeoutInWaitForReplicas = true;

        private QueryStringParameters([JetBrains.Annotations.NotNull] HttpRequest httpRequest)
            : base(httpRequest)
        {
        }

        protected override void OnFinalize()
        {
            if (AnyStringValues() == false)
                return;

            WaitForSpecificIndexes = ConvertToStringValues("waitForSpecificIndex");
        }

        protected override void OnValue(QueryStringEnumerable.EncodedNameValuePair pair)
        {
            var name = pair.EncodedName;

            if (IsMatch(name, NoReplyQueryStringName))
            {
                NoReply = GetBoolValue(name, pair.EncodedValue);
                return;
            }

            if (IsMatch(name, WaitForIndexesTimeoutQueryStringName))
            {
                WaitForIndexesTimeout = GetTimeSpan(name, pair.EncodedValue);
                return;
            }

            if (IsMatch(name, WaitForIndexThrowQueryStringName))
            {
                WaitForIndexThrow = GetBoolValue(name, pair.EncodedValue);
                return;
            }

            if (IsMatch(name, WaitForSpecificIndexQueryStringName))
            {
                AddForStringValues("waitForSpecificIndex", pair.DecodeValue());
                return;
            }

            if (IsMatch(name, WaitForReplicasTimeoutQueryStringName))
            {
                WaitForReplicasTimeout = GetTimeSpan(name, pair.EncodedValue);
                return;
            }

            if (IsMatch(name, NumberOfReplicasToWaitForQueryStringName))
            {
                var value = pair.DecodeValue();
                if (IsMatch(value, MajorityValue))
                {
                    Majority = true;
                    return;
                }

                NumberOfReplicasToWaitFor = GetIntValue(name, value);
                return;
            }

            if (IsMatch(name, ThrowOnTimeoutInWaitForReplicasQueryStringName))
            {
                ThrowOnTimeoutInWaitForReplicas = GetBoolValue(name, pair.EncodedValue);
                return;
            }
        }

        public static QueryStringParameters Create(HttpRequest httpRequest)
        {
            var parameters = new QueryStringParameters(httpRequest);
            parameters.Parse();

            return parameters;
        }
    }
}

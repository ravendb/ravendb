using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Text;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands.Batches
{
    internal class ClusterWideBatchCommand : SingleNodeBatchCommand, IRaftCommand
    {
        public bool? DisableAtomicDocumentWrites { get; }
        public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();

        public ClusterWideBatchCommand(DocumentConventions conventions, IList<ICommandData> commands, BatchOptions options = null, bool? disableAtomicDocumentsWrites = null) : 
            base(conventions, context: null, commands, options, TransactionMode.ClusterWide)
        {
            DisableAtomicDocumentWrites = disableAtomicDocumentsWrites;
        }

        protected override void AppendOptions(StringBuilder sb)
        {
            base.AppendOptions(sb);
            if (DisableAtomicDocumentWrites == null)
                return;
            sb.Append("&disableAtomicDocumentWrites=")
                .Append(DisableAtomicDocumentWrites.Value ? "true" : "false");
        }
    }

    public class SingleNodeBatchCommand : RavenCommand<BatchCommandResult>, IDisposable
    {
        private BlittableJsonReaderObject[] _commandsAsJson;
        private bool? _supportsAtomicWrites;
        private readonly List<Stream> _attachmentStreams;
        private readonly HashSet<Stream> _uniqueAttachmentStreams;
        private readonly DocumentConventions _conventions;
        private readonly IList<ICommandData> _commands;
        private readonly BatchOptions _options;
        private readonly TransactionMode _mode;

        public SingleNodeBatchCommand(DocumentConventions conventions, JsonOperationContext context, IList<ICommandData> commands, BatchOptions options = null, TransactionMode mode = TransactionMode.SingleNode)
        {
            _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
            _commands = commands ?? throw new ArgumentNullException(nameof(commands));
            _options = options;
            _mode = mode;

            _commandsAsJson = new BlittableJsonReaderObject[_commands.Count];
            foreach (var command in commands)
            {
                if (command is PutAttachmentCommandData putAttachmentCommandData)
                {
                    if (_attachmentStreams == null)
                    {
                        _attachmentStreams = new List<Stream>();
                        _uniqueAttachmentStreams = new HashSet<Stream>();
                    }

                    var stream = putAttachmentCommandData.Stream;
                    PutAttachmentCommandHelper.ValidateStream(stream);
                    if (_uniqueAttachmentStreams.Add(stream) == false)
                        PutAttachmentCommandHelper.ThrowStreamWasAlreadyUsed();
                    _attachmentStreams.Add(stream);
                }
            }
            
            Timeout = options?.RequestTimeout;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            if (_supportsAtomicWrites == null ||
                node.SupportsAtomicClusterWrites != _supportsAtomicWrites)
            {
                _supportsAtomicWrites = node.SupportsAtomicClusterWrites;
                for (var i = 0; i < _commands.Count; i++)
                {
                    var command = _commands[i];
                  
                    var json = command.ToJson(_conventions, ctx);

                    if (node.SupportsAtomicClusterWrites == false)
                    {   // support older clients
                        json.RemoveInMemoryPropertyByName(nameof(PutCommandData.OriginalChangeVector));
                    }
                    _commandsAsJson[i] = ctx.ReadObject(json, "command");
                }
            }
            
            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Post,
                Content = new BlittableJsonContent(async stream =>
                {
                    await using (var writer = new AsyncBlittableJsonTextWriter(ctx, stream))
                    {
                        writer.WriteStartObject();
                        writer.WriteArray("Commands", _commandsAsJson);
                        if (_mode == TransactionMode.ClusterWide)
                        {
                            writer.WriteComma();
                            writer.WritePropertyName(nameof(TransactionMode));
                            writer.WriteString(nameof(TransactionMode.ClusterWide));
                        }
                        writer.WriteEndObject();
                    }
                })
            };

            if (_attachmentStreams != null && _attachmentStreams.Count > 0)
            {
                var multipartContent = new MultipartContent { request.Content };
                foreach (var stream in _attachmentStreams)
                {
                    PutAttachmentCommandHelper.PrepareStream(stream);
                    var streamContent = new AttachmentStreamContent(stream, CancellationToken);
                    streamContent.Headers.TryAddWithoutValidation("Command-Type", "AttachmentStream");
                    multipartContent.Add(streamContent);
                }
                request.Content = multipartContent;
            }

            var sb = new StringBuilder($"{node.Url}/databases/{node.Database}/bulk_docs?");

            AppendOptions(sb);

            url = sb.ToString();

            return request;
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                throw new InvalidOperationException("Got null response from the server after doing a batch, something is very wrong. Probably a garbled response.");
            // this should never actually occur, we are not caching the response of batch commands, but keeping it here anyway
            if (fromCache)
            {
                // we have to clone the response here because  otherwise the cached item might be freed while
                // we are still looking at this result, so we clone it to the side
                response = response.Clone(context);
            }
            Result = JsonDeserializationClient.BatchCommandResult(response);
        }

        protected virtual void AppendOptions(StringBuilder sb)
        {
            if (_options == null)
                return;

            var replicationOptions = _options.ReplicationOptions;
            if (replicationOptions != null)
            {
                sb.Append("&waitForReplicasTimeout=").Append(replicationOptions.WaitForReplicasTimeout);

                sb.Append($"&throwOnTimeoutInWaitForReplicas={replicationOptions.ThrowOnTimeoutInWaitForReplicas}");

                sb.Append("&numberOfReplicasToWaitFor=");
                sb.Append(replicationOptions.Majority
                    ? "majority"
                    : replicationOptions.NumberOfReplicasToWaitFor.ToString());
            }

            var indexOptions = _options.IndexOptions;
            if (indexOptions != null)
            {
                sb.Append("&waitForIndexesTimeout=").Append(indexOptions.WaitForIndexesTimeout);
                sb.Append("&waitForIndexThrow=").Append(indexOptions.ThrowOnTimeoutInWaitForIndexes.ToString());
                if (indexOptions.WaitForSpecificIndexes != null)
                {
                    foreach (var specificIndex in indexOptions.WaitForSpecificIndexes)
                    {
                        sb.Append("&waitForSpecificIndex=").Append(Uri.EscapeDataString(specificIndex));
                    }
                }
            }
        }

        public override bool IsReadRequest => false;

        public void Dispose()
        {
            foreach (var command in _commandsAsJson)
                command?.Dispose();

            Result?.Results?.Dispose();
        }
    }
}

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
    internal sealed class ClusterWideBatchCommand : SingleNodeBatchCommand, IRaftCommand
    {
        public bool? DisableAtomicDocumentWrites { get; }
        public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();

        public ClusterWideBatchCommand(DocumentConventions conventions, IList<ICommandData> commands, BatchOptions options = null, bool? disableAtomicDocumentsWrites = null)
            : base(conventions, commands, options, TransactionMode.ClusterWide)
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

    internal class SingleNodeBatchWithTrackingCommand : SingleNodeBatchCommand
    {
        private readonly BatchTrackChangesCommandData _trackChangesCommand;

        public SingleNodeBatchWithTrackingCommand(DocumentConventions conventions,  IList<ICommandData> commands, BatchTrackChangesCommandData trackChangesCommand, 
            BatchOptions options = null, TransactionMode mode = TransactionMode.SingleNode) : base(conventions, commands, options, mode)
        {
            _trackChangesCommand = trackChangesCommand;
        }

        protected override IList<ICommandData> Initialize(IList<ICommandData> commands)
        {
            if (_trackChangesCommand == null) 
                return base.Initialize(commands);

            var cmds = new List<ICommandData>{ _trackChangesCommand };
            foreach (var command in commands)
            {
                HandlePutAttachmentCommandData(command);
                cmds.Add(command);
            }

            _commandsAsJson = new BlittableJsonReaderObject[cmds.Count];

            return cmds;

        }
    }

    public class SingleNodeBatchCommand : RavenCommand<BatchCommandResult>, IDisposable
    {
        protected BlittableJsonReaderObject[] _commandsAsJson;
        private bool? _supportsAtomicWrites;
        private HashSet<Stream> _uniqueAttachmentStreams;
        private readonly DocumentConventions _conventions;
        private readonly IList<ICommandData> _commands;
        private readonly BatchOptions _options;
        private readonly TransactionMode _mode;

        public SingleNodeBatchCommand(DocumentConventions conventions, IList<ICommandData> commands, BatchOptions options = null)
            : this(conventions, commands, options, TransactionMode.SingleNode)
        {
        }

        protected SingleNodeBatchCommand(DocumentConventions conventions, IList<ICommandData> commands, BatchOptions options, TransactionMode mode)
        {
            _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
            _commands = commands ?? throw new ArgumentNullException(nameof(commands));
            _options = options;
            _mode = mode;

            Timeout = options?.RequestTimeout;
        }

        protected virtual IList<ICommandData> Initialize(IList<ICommandData> commands)
        {
            _commandsAsJson = new BlittableJsonReaderObject[commands.Count];
            foreach (var command in commands)
            {
                HandlePutAttachmentCommandData(command);
            }

            return commands;
        }

        protected void HandlePutAttachmentCommandData(ICommandData command)
        {
            if (command is not PutAttachmentCommandData putAttachmentCommandData) 
                return;

            if (PutAttachmentCommandHelper.TryValidateStream(putAttachmentCommandData.Stream, putAttachmentCommandData.RemoteParameters) == false)
                return;

            _uniqueAttachmentStreams ??= new HashSet<Stream>();

            var stream = putAttachmentCommandData.Stream;
            if (_uniqueAttachmentStreams.Add(stream) == false)
                PutAttachmentCommandHelper.ThrowStreamWasAlreadyUsed();
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            //TODO: egor the best would be to combine the loop in Initialize with if (_supportsAtomicWrites == null)
            var commands = Initialize(_commands);

            if (_supportsAtomicWrites == null)
            {
                _supportsAtomicWrites = node.SupportsAtomicClusterWrites;

                for (var i = 0; i < commands.Count; i++)
                {
                    var command = commands[i];

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
                }, _conventions)
            };

            if (_uniqueAttachmentStreams != null && _uniqueAttachmentStreams.Count > 0)
            {
                var multipartContent = new MultipartContent { request.Content };
                foreach (var stream in _uniqueAttachmentStreams)
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

            AppendOptions(sb, _options.IndexOptions, _options.ReplicationOptions, _options.ShardedOptions);
        }

        internal static void AppendOptions(StringBuilder sb, IndexBatchOptions indexOptions, ReplicationBatchOptions replicationOptions, ShardedBatchOptions shardedOptions)
        {
            if (replicationOptions != null)
            {
                sb.Append("&waitForReplicasTimeout=").Append(replicationOptions.WaitForReplicasTimeout);

                sb.Append($"&throwOnTimeoutInWaitForReplicas={replicationOptions.ThrowOnTimeoutInWaitForReplicas}");

                sb.Append("&numberOfReplicasToWaitFor=");
                sb.Append(replicationOptions.Majority
                    ? "majority"
                    : replicationOptions.NumberOfReplicasToWaitFor.ToString());
            }

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

            if (shardedOptions != null)
            {
                if (shardedOptions.BatchBehavior != ShardedBatchBehavior.Default)
                    sb.Append("&shardedBatchBehavior=").Append(shardedOptions.BatchBehavior);
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

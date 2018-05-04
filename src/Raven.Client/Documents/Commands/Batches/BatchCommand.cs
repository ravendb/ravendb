using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands.Batches
{
    public class BatchCommand : RavenCommand<BlittableArrayResult>, IDisposable
    {
        private readonly BlittableJsonReaderObject[] _commands;
        private readonly HashSet<Stream> _attachmentStreams;
        private readonly BatchOptions _options;

        public BatchCommand(DocumentConventions conventions, JsonOperationContext context, List<ICommandData> commands, BatchOptions options = null)
        {
            if (conventions == null)
                throw new ArgumentNullException(nameof(conventions));
            if (commands == null)
                throw new ArgumentNullException(nameof(commands));
            if (context == null)
                throw new ArgumentNullException(nameof(context));

            _commands = new BlittableJsonReaderObject[commands.Count];
            for (var i = 0; i < commands.Count; i++)
            {
                var command = commands[i];
                _commands[i] = context.ReadObject(command.ToJson(conventions, context), "command");

                if (command is PutAttachmentCommandData putAttachmentCommandData)
                {
                    if (_attachmentStreams == null)
                        _attachmentStreams = new HashSet<Stream>();

                    var stream = putAttachmentCommandData.Stream;
                    PutAttachmentCommandHelper.ValidateStream(stream);
                    if (_attachmentStreams.Add(stream) == false)
                        PutAttachmentCommandHelper.ThrowStreamAlready();
                }
            }

            _options = options;

            Timeout = options?.RequestTimeout;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Post,
                Content = new BlittableJsonContent(stream =>
                {
                    using (var writer = new BlittableJsonTextWriter(ctx, stream))
                    {
                        writer.WriteStartObject();
                        writer.WriteArray("Commands", _commands);
                        writer.WriteEndObject();
                    }
                })
            };

            if (_attachmentStreams != null && _attachmentStreams.Count > 0)
            {
                var multipartContent = new MultipartContent {request.Content};
                foreach (var stream in _attachmentStreams)
                {
                    PutAttachmentCommandHelper.PrepareStream(stream);
                    var streamContent = new AttachmentStreamContent(stream, CancellationToken);
                    streamContent.Headers.TryAddWithoutValidation("Command-Type", "AttachmentStream");
                    multipartContent.Add(streamContent);
                }
                request.Content = multipartContent;
            }

            var sb = new StringBuilder($"{node.Url}/databases/{node.Database}/bulk_docs");

            AppendOptions(sb);

            url = sb.ToString();

            return request;
        }

        public override Task<ResponseDisposeHandling> ProcessResponse(JsonOperationContext context, HttpCache cache, HttpResponseMessage response, string url)
        {
            if (response.Headers.TryGetValues("TransactionIndex", out var value))
            {
                if (long.TryParse(value.FirstOrDefault(), out var index))
                {
                    ReturnTransactionIndex = index;
                }
            }
            return base.ProcessResponse(context, cache, response, url);
        }

        public long ReturnTransactionIndex { get; private set; }

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
            Result = JsonDeserializationClient.BlittableArrayResult(response);
        }

        private void AppendOptions(StringBuilder sb)
        {
            if (_options == null)
                return;

            sb.AppendLine("?");

            if (_options.TryGetValue<ReplicationBatchOptions>(out var replicationOptions))
            {
                sb.Append("&waitForReplicasTimeout=").Append(replicationOptions.WaitForReplicasTimeout);

                if (replicationOptions.ThrowOnTimeoutInWaitForReplicas)
                    sb.Append("&throwOnTimeoutInWaitForReplicas=true");

                sb.Append("&numberOfReplicasToWaitFor=");
                sb.Append(replicationOptions.Majority
                    ? "majority"
                    : replicationOptions.NumberOfReplicasToWaitFor.ToString());
            }

            if (_options.TryGetValue<IndexBatchOptions>(out var indexOptions))
            {
                sb.Append("&waitForIndexesTimeout=").Append(indexOptions.WaitForIndexesTimeout);
                sb.Append("&waitForIndexThrow=").Append(indexOptions.ThrowOnTimeoutInWaitForIndexes.ToString());
                if (indexOptions.WaitForSpecificIndexes != null)
                {
                    foreach (var specificIndex in indexOptions.WaitForSpecificIndexes)
                    {
                        sb.Append("&waitForSpecificIndex=").Append(specificIndex);
                    }
                }
            }

            if (_options.TryGetValue<ClusterBatchOptions>(out var clusterBatchOptions))
            {
                sb.Append("&clusterTransaction=true");
            }
        }

        public override bool IsReadRequest => false;

        public void Dispose()
        {
            foreach (var command in _commands)
                command?.Dispose();
        }
    }
}

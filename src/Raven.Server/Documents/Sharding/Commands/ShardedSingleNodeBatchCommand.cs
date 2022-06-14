using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Text;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Server.Documents.Sharding.Handlers.Batches;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Commands;

public class ShardedSingleNodeBatchCommand : RavenCommand<BlittableJsonReaderObject>
{
    private readonly IndexBatchOptions _indexBatchOptions;
    private readonly ReplicationBatchOptions _replicationBatchOptions;

    private readonly List<Stream> _commands = new List<Stream>();
    private readonly List<int> _positionInResponse = new List<int>();

    private List<Stream> _attachmentStreams;
    private HashSet<Stream> _uniqueAttachmentStreams;

    public ShardedSingleNodeBatchCommand(IndexBatchOptions indexBatchOptions, ReplicationBatchOptions replicationBatchOptions)
    {
        _indexBatchOptions = indexBatchOptions;
        _replicationBatchOptions = replicationBatchOptions;
    }

    public void AddCommand(SingleShardedCommand command)
    {
        _commands.Add(command.CommandStream);
        _positionInResponse.Add(command.PositionInResponse);

        if (command.AttachmentStream != null)
        {
            var stream = command.AttachmentStream;
            if (_attachmentStreams == null)
            {
                _attachmentStreams = new List<Stream>();
                _uniqueAttachmentStreams = new HashSet<Stream>();
            }

            if (_uniqueAttachmentStreams.Add(stream) == false)
                PutAttachmentCommandHelper.ThrowStreamWasAlreadyUsed();
            _attachmentStreams.Add(stream);
        }
    }

    public void AssembleShardedReply(JsonOperationContext context, object[] reply)
    {
        Result.TryGet(nameof(BatchCommandResult.Results), out BlittableJsonReaderArray partialResult);
        var count = 0;
        foreach (var o in partialResult.Items)
        {
            var positionInResult = _positionInResponse[count++];
            if (o is BlittableJsonReaderObject blittable)
            {
                reply[positionInResult] = blittable.Clone(context);
                continue;
            }
            if (o is BlittableJsonReaderArray blittableArray)
            {
                reply[positionInResult] = blittableArray.Clone(context);
                continue;
            }
            reply[positionInResult] = o;
        }
    }

    public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
    {
        var sb = new StringBuilder($"{node.Url}/databases/{node.Database}/bulk_docs?");

        SingleNodeBatchCommand.AppendOptions(sb, _indexBatchOptions, _replicationBatchOptions);

        url = sb.ToString();

        var request = new HttpRequestMessage
        {
            Method = HttpMethod.Post,
            Content = new BlittableJsonContent(async stream =>
            {
                await using (var writer = new AsyncBlittableJsonTextWriter(ctx, stream))
                {
                    writer.WriteStartObject();

                    await writer.WriteArrayAsync("Commands", _commands);

                    writer.WriteEndObject();
                }
            })
        };

        if (_attachmentStreams is { Count: > 0 })
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

        return request;
    }

    public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
    {
        Result = response;
    }

    public override bool IsReadRequest => false;

    public void Dispose()
    {
        foreach (var command in _commands)
            command?.Dispose();

        if (_uniqueAttachmentStreams != null)
        {
            foreach (var uniqueAttachmentStream in _uniqueAttachmentStreams)
                uniqueAttachmentStream?.Dispose();
        }

        Result?.Dispose();
    }
}

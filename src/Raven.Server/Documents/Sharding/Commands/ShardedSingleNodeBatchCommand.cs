using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using Raven.Client;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Server.Documents.Sharding.Handlers.Batches;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Sharding.Commands;

public class ShardedSingleNodeBatchCommand : RavenCommand<BlittableJsonReaderObject>
{
    private readonly DocumentConventions _conventions;
    public readonly int ShardNumber;
    private readonly IndexBatchOptions _indexBatchOptions;
    private readonly ReplicationBatchOptions _replicationBatchOptions;

    public readonly List<SingleShardedCommand> Commands = new List<SingleShardedCommand>();
    public readonly List<int> PositionInResponse = new List<int>();

    private List<Stream> _attachmentStreams;
    private HashSet<Stream> _uniqueAttachmentStreams;

    public ShardedSingleNodeBatchCommand(DocumentConventions conventions, int shardNumber, IndexBatchOptions indexBatchOptions, ReplicationBatchOptions replicationBatchOptions)
    {
        _conventions = conventions;
        ShardNumber = shardNumber;
        _indexBatchOptions = indexBatchOptions;
        _replicationBatchOptions = replicationBatchOptions;
    }

    public void AddCommand(SingleShardedCommand command)
    {
        Commands.Add(command);
        PositionInResponse.Add(command.PositionInResponse);

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

    public void AssembleShardedReply(JsonOperationContext context, object[] reply, int? shardNumber)
    {
        Result.TryGet(nameof(BatchCommandResult.Results), out BlittableJsonReaderArray partialResult);
        var count = 0;
        foreach (var o in partialResult.Items)
        {
            var positionInResult = PositionInResponse[count++];
            if (o is BlittableJsonReaderObject blittable)
            {
                if (shardNumber.HasValue)
                {
                    blittable.Modifications = new DynamicJsonValue
                    {
                        [Constants.Documents.Metadata.Sharding.ShardNumber] = shardNumber.Value
                    };
                }
                
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

        SingleNodeBatchCommand.AppendOptions(sb, _indexBatchOptions, _replicationBatchOptions, shardedOptions: null);

        url = sb.ToString();

        var request = new HttpRequestMessage
        {
            Method = HttpMethod.Post,
            Content = new BlittableJsonContent(async stream =>
            {
                await using (var writer = new AsyncBlittableJsonTextWriter(ctx, stream))
                {
                    writer.WriteStartObject();

                    await writer.WriteArrayAsync("Commands", Commands.Select(c => c.CommandStream));

                    writer.WriteEndObject();
                }
            }, _conventions)
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
        foreach (var command in Commands)
            command.CommandStream?.Dispose();

        if (_uniqueAttachmentStreams != null)
        {
            foreach (var uniqueAttachmentStream in _uniqueAttachmentStreams)
                uniqueAttachmentStream?.Dispose();
        }

        Result?.Dispose();
    }
}

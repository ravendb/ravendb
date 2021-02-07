using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Server.Documents.Handlers;
using Raven.Server.Documents.ShardedHandlers.ShardedCommands;
using Raven.Server.Documents.Sharding;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.TrafficWatch;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ShardedHandlers
{
    public class ShardedBatchHandler : ShardedRequestHandler
    {
        [RavenShardedAction("/databases/*/bulk_docs", "POST")]
        public async Task BulkDocs()
        {
            var commandBuilder = new ShardedBatchCommandBuilder(this);
            using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var contentType = HttpContext.Request.ContentType;
                if (contentType == null ||
                    contentType.StartsWith("application/json", StringComparison.OrdinalIgnoreCase))
                {
                    await commandBuilder.BuildCommandsAsync(context, RequestBodyStream());
                }
                else if (contentType.StartsWith("multipart/mixed", StringComparison.OrdinalIgnoreCase) ||
                         contentType.StartsWith("multipart/form-data", StringComparison.OrdinalIgnoreCase))
                {
                    await commandBuilder.ParseMultipart(context, RequestBodyStream(), HttpContext.Request.ContentType);
                }
                else
                    BatchHandler.ThrowNotSupportedType(contentType);

                if (TrafficWatchManager.HasRegisteredClients)
                {
                    var log = BatchHandler.BatchTrafficWatch(commandBuilder.Commands);
                    // add sb to httpContext
                    AddStringToHttpContext(log, TrafficWatchChangeType.BulkDocs); // TODO sharding: maybe ShardedBulkDocs type?
                }

                using (var batch = await commandBuilder.GetCommand(context))
                {
                    if (batch.IsClusterTransaction)
                    {
                        throw new NotSupportedException("Cluster transactions are currently not supported in a sharded database.");
                    }

                    var shardedBatchCommands = new Dictionary<int, SingleNodeShardedBatchCommand>(); // TODO sharding : consider cache those
                    foreach (var command in batch)
                    {
                        var id = command.Id;
                        var shardIndex = ShardedContext.GetShardIndex(context, id);
                        var requestExecutor = ShardedContext.RequestExecutors[shardIndex];

                        if (shardedBatchCommands.TryGetValue(shardIndex, out var shardedBatchCommand) == false)
                        {
                            shardedBatchCommand = new SingleNodeShardedBatchCommand(this, requestExecutor.ContextPool);
                            shardedBatchCommands.Add(shardIndex, shardedBatchCommand);
                        }

                        shardedBatchCommand.AddCommand(command);
                    }

                    var tasks = new List<Task>();
                    foreach (var command in shardedBatchCommands)
                    {
                        tasks.Add(ShardedContext.RequestExecutors[command.Key].ExecuteAsync(command.Value, command.Value.Context));
                    }

                    await Task.WhenAll(tasks);

                    HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;
                    using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        var reply = new object[batch.ParsedCommands.Count];

                        foreach (var command in shardedBatchCommands.Values)
                        {
                            var count = 0;
                            var result = command.Result;
                            result.TryGet(nameof(BatchCommandResult.Results), out BlittableJsonReaderArray partialResult);
                            foreach (var o in partialResult.Items)
                            {
                                var positionInResult = command.PositionInResponse[count++];
                                reply[positionInResult] = o;
                            }
                        }
                        context.Write(writer, new DynamicJsonValue {[nameof(BatchCommandResult.Results)] = new DynamicJsonArray(reply)});
                    }
                }
            }
        }
    }

    public class SingleNodeShardedBatchCommand : ShardedCommand
    {
        private readonly JsonOperationContext _context;
        private readonly List<Stream> _commands = new List<Stream>();
        public List<int> PositionInResponse = new List<int>();

        private List<Stream> _attachmentStreams;
        private HashSet<Stream> _uniqueAttachmentStreams;
        private readonly TransactionMode _mode = TransactionMode.SingleNode;
        private readonly IDisposable _returnCtx;
        public JsonOperationContext Context => _context;

        public SingleNodeShardedBatchCommand(ShardedRequestHandler handler, JsonContextPool pool) :
            base(handler, ShardedCommands.Headers.None)
        {
            _returnCtx = pool.AllocateOperationContext(out _context);
        }

        public void AddCommand(SingleShardedCommand command)
        {
            _commands.Add(command.BufferedCommand);
            PositionInResponse.Add(command.PositionInResponse);

            if (command.AttachmentStream != null)
            {
                var stream = command.AttachmentStream;
                if (_attachmentStreams == null)
                {
                    _attachmentStreams = new List<Stream>();
                    _uniqueAttachmentStreams = new HashSet<Stream>();
                }

                PutAttachmentCommandHelper.ValidateStream(stream);
                if (_uniqueAttachmentStreams.Add(stream) == false)
                    PutAttachmentCommandHelper.ThrowStreamWasAlreadyUsed();
                _attachmentStreams.Add(stream);
            }
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            var request = base.CreateRequest(ctx, node, out url);

            request.Content = new BlittableJsonContent(stream =>
            {
                using (var writer = new BlittableJsonTextWriter(ctx, stream))
                {
                    writer.WriteStartObject();
                    writer.WriteArray("Commands", _commands);
                    if (_mode == TransactionMode.ClusterWide)
                    {
                        writer.WriteComma();
                        writer.WritePropertyName(nameof(TransactionMode));
                        writer.WriteString(nameof(TransactionMode.ClusterWide));
                    }

                    writer.WriteEndObject();
                }
            });

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

            return request;
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

            _returnCtx?.Dispose();
        }
    }

    public class SingleShardedCommand
    {
        public string Id;
        public Stream AttachmentStream;
        public MemoryStream BufferedCommand;
        public int PositionInResponse;
    }

    public class ShardedBatchCommand : IEnumerator<SingleShardedCommand>, IEnumerable<SingleShardedCommand>
    {
        public List<MemoryStream> BufferedCommands;
        public ArraySegment<BatchRequestParser.CommandData> ParsedCommands;
        public List<Stream> AttachmentStreams;
        public bool IsClusterTransaction;

        private int _commandPosition;
        private int _streamPosition;

        public bool MoveNext()
        {
            if (_commandPosition == ParsedCommands.Count)
                return false;

            var cmd = ParsedCommands[_commandPosition];
            var stream = cmd.Type == CommandType.AttachmentPUT ? AttachmentStreams[_streamPosition++] : null;

            _current = new SingleShardedCommand
            {
                Id = cmd.Id,
                AttachmentStream = stream,
                BufferedCommand = BufferedCommands[_commandPosition],
                PositionInResponse = _commandPosition
            };
            _commandPosition++;
            return true;
        }

        public void Reset()
        {
            throw new NotSupportedException();
        }

        private SingleShardedCommand _current;

        public SingleShardedCommand Current => _current;

        object IEnumerator.Current => _current;

        public void Dispose()
        {
        }

        public IEnumerator<SingleShardedCommand> GetEnumerator()
        {
            return this;
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }

    public class ShardedBatchCommandBuilder : BatchRequestParser.BatchCommandBuilder
    {
        public List<Stream> Streams;
        public List<BufferedCommand> BufferedCommands = new List<BufferedCommand>();

        private readonly bool _encrypted;

        public ShardedBatchCommandBuilder(ShardedRequestHandler handler) :
            base(handler, handler.ShardedContext.DatabaseName, handler.ShardedContext.IdentitySeparator)
        {
            _encrypted = handler.ShardedContext.Encrypted;
        }

        public override async Task SaveStream(JsonOperationContext context, Stream input)
        {
            Streams ??= new List<Stream>();
            var attachment = GetServerTempFile("sharded").StartNewStream();
            await input.CopyToAsync(attachment, Handler.AbortRequestToken);
            await attachment.FlushAsync(Handler.AbortRequestToken);
            Streams.Add(attachment);
        }

        public StreamsTempFile GetServerTempFile(string prefix)
        {
            var name = $"attachment.{Guid.NewGuid():N}.{prefix}";
            var tempPath = ServerStore._env.Options.DataPager.Options.TempPath.Combine(name);
            
            return new StreamsTempFile(tempPath.FullPath, _encrypted);
        }

        public override async Task<BatchRequestParser.CommandData> ReadCommand(
            JsonOperationContext ctx, 
            Stream stream, JsonParserState state, 
            UnmanagedJsonParser parser, 
            JsonOperationContext.MemoryBuffer buffer, 
            BlittableMetadataModifier modifier,
            CancellationToken token)
        {
            var ms = new MemoryStream();
            try
            {
                var bufferedCommand = new BufferedCommand {CommandStream = ms};
                var result = await BatchRequestParser.ReadAndCopySingleCommand(ctx, stream, state, parser, buffer, bufferedCommand, token);
                BufferedCommands.Add(bufferedCommand);
                return result;
            }
            catch
            {
                await ms.DisposeAsync();
                throw;
            }
        }

        private IEnumerable<MemoryStream> GetStreams()
        {
            var orderedList = _identityPositions?.OrderBy(x => x).ToList();

            for (var index = 0; index < BufferedCommands.Count; index++)
            {
                var bufferedCommand = BufferedCommands[index];
                if (orderedList?.Contains(index) != true)
                {
                    yield return bufferedCommand.CommandStream;
                    continue;
                }

                yield return bufferedCommand.ModifyIdentityStream(Commands[index].Id);
            }
        }

        public async Task<ShardedBatchCommand> GetCommand(JsonOperationContext ctx)
        {
            await ExecuteGetIdentities();
            return new ShardedBatchCommand
            {
                ParsedCommands = Commands, 
                BufferedCommands = GetStreams().ToList(),
                AttachmentStreams = Streams, 
                IsClusterTransaction = IsClusterTransactionRequest
            };
        }

        public class BufferedCommand
        {
            public MemoryStream CommandStream;

            // for identities we should replace the id and change vector
            public int IdStartPosition; 
            public int ChangeVectorPosition; 
            public int IdLength;

            public MemoryStream ModifyIdentityStream(string newId)
            {
                CommandStream.Position = 0;

                var newStream = new MemoryStream();
                var modifier = new BufferedCommandModifier
                {
                    ChangeVectorPosition = ChangeVectorPosition, 
                    IdStartPosition = IdStartPosition, 
                    IdLength = IdLength
                };
                modifier.Initialize();
                modifier.Rewrite(CommandStream, newStream, newId);
               
                CommandStream.Dispose();
                return newStream;
            }

            private struct BufferedCommandModifier
            {
                public int IdStartPosition;
                public int ChangeVectorPosition;
                public int IdLength;

                private Item[] _order;
                private enum Item
                {
                    None,
                    Id,
                    ChangeVector
                }

                public void Initialize()
                {
                    if (IdStartPosition <= 0)
                        ThrowInvalidArgument("Id position");

                    if (IdLength <= 0)
                        ThrowInvalidArgument("Id length");

                    if (ChangeVectorPosition <= 0)
                        ThrowInvalidArgument("Change vector position");

                    _order = new Item[2];

                    if (ChangeVectorPosition < IdStartPosition)
                    {
                        _order[0] = Item.ChangeVector;
                        _order[1] = Item.Id;
                    }
                    else
                    {
                        _order[1] = Item.ChangeVector;
                        _order[0] = Item.Id;
                    }
                }

                public void Rewrite(MemoryStream source, MemoryStream dest, string newId)
                {
                    var sourceBuffer = source.GetBuffer();
                    var offset = 0;

                    foreach (var item in _order)
                    {
                        switch (item)
                        {
                            case Item.Id:
                                offset = WriteRemaining(IdStartPosition);

                                // we need to replace here the piped id with the actual one
                                dest.Write(Encoding.UTF8.GetBytes(newId));
                                offset += IdLength;

                                break;
                            case Item.ChangeVector:
                                offset = WriteRemaining(ChangeVectorPosition);
                                
                                // change vector must be null for identity request and string.Empty after generation
                                dest.Write(Empty);
                                offset += 4; // null

                                break;
                            default:
                                throw new ArgumentException(item.ToString());
                        }

                        int WriteRemaining(int upto)
                        {
                            var remaining = upto - offset;
                            if (remaining < 0)
                                throw new InvalidOperationException();

                            if (remaining > 0)
                            {
                                dest.Write(sourceBuffer, offset, remaining);
                                offset += remaining;
                            }

                            return offset;
                        }
                    }
                
                    // copy the rest
                    source.Position = offset;
                    source.CopyTo(dest);
                }

                private static byte[] Empty = Encoding.UTF8.GetBytes("\"\"");

                private static void ThrowInvalidArgument(string name)
                {
                    throw new ArgumentException($"{name} must be positive");
                }
            }
        }
    }
}

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
            var commandBuilder = new ShardedBatchCommandBuilder(this, ShardedContext.DatabaseName, ShardedContext.IdentitySeparator);
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

                    var shardedBatchCommands = new Dictionary<int, SingleNodeShardedBatchCommand>();
                    foreach (var command in batch)
                    {
                        var id = command.Id;
                        var shardIndex = ShardedContext.GetShardIndex(context, id);
                        var requestExecutor = ShardedContext.RequestExecutors[shardIndex];

                        if (shardedBatchCommands.ContainsKey(shardIndex) == false)
                        {
                            shardedBatchCommands.Add(shardIndex, new SingleNodeShardedBatchCommand(this, requestExecutor.ContextPool));
                        }

                        var shardedBatchCommand = shardedBatchCommands[shardIndex];
                        shardedBatchCommand.AddCommand(command);
                    }

                    var tasks = shardedBatchCommands.Select(x => ShardedContext.RequestExecutors[x.Key].ExecuteAsync(x.Value, x.Value.Context));
                    await Task.WhenAll(tasks);

                    HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;
                    using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        var reply = new DynamicJsonArray();
                        foreach (var command in shardedBatchCommands.Values)
                        {
                            var result = command.Result;
                            result.TryGet(nameof(BatchCommandResult.Results), out BlittableJsonReaderArray partialResult);
                            foreach (var o in partialResult.Items)
                            {
                                reply.Add(o);
                            }
                        }
                        context.Write(writer, new DynamicJsonValue {[nameof(BatchCommandResult.Results)] = reply});
                    }
                }
            }
        }
    }

    public class SingleNodeShardedBatchCommand : ShardedCommand
    {
        private readonly JsonOperationContext _context;
        private readonly List<Stream> _commands = new List<Stream>();
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
                BufferedCommand = BufferedCommands[_commandPosition]
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

        public ShardedBatchCommandBuilder(RequestHandler handler, string database, char identityPartsSeparator) :
            base(handler, database, identityPartsSeparator)
        {
        }

        public override async Task SaveStream(JsonOperationContext context, Stream input)
        {
            Streams ??= new List<Stream>();

            var ms = new MemoryStream();
            await input.CopyToAsync(ms, CancellationToken.None); // TODO sharding: pass cancellation token
            Streams.Add(ms);
        }

        public override async Task<BatchRequestParser.CommandData> ReadCommand(JsonOperationContext ctx, Stream stream, JsonParserState state, UnmanagedJsonParser parser, JsonOperationContext.MemoryBuffer buffer, BlittableMetadataModifier modifier,
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

            public long IdStartPosition; // id should be replace for identities
            public long ChangeVectorPosition; // change vector must be null for identity request and string.Empty after generation

            public int IdLength;

            public MemoryStream ModifyIdentityStream(string newId)
            {
                // we need to replace here the piped id with the actual one

                CommandStream.Position = 0;

                var ms = new MemoryStream();

                var source = CommandStream.GetBuffer();
                
                // TODO: re-write the change vector to use string.Empty
                ms.Write(source, 0, (int)IdStartPosition);
                ms.Write(Encoding.UTF8.GetBytes(newId));
                
                CommandStream.Position = IdStartPosition + IdLength;
                CommandStream.CopyTo(ms);
                CommandStream.Dispose();
                return ms;
            }
        }
    }
}

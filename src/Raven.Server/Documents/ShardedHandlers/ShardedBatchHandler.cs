using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
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
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Json.Sync;

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
                        var shardIndex = command.Shard;
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
                            command.AssembleShardedReply(reply);
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
        private readonly List<int> _positionInResponse = new List<int>();

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

        public void AssembleShardedReply(object[] reply)
        {
            Result.TryGet(nameof(BatchCommandResult.Results), out BlittableJsonReaderArray partialResult);
            var count = 0;
            foreach (var o in partialResult.Items)
            {
                var positionInResult = _positionInResponse[count++];
                reply[positionInResult] = o;
            }
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            var request = base.CreateRequest(ctx, node, out url);

            request.Content = new BlittableJsonContent(async stream =>
            {
                await using (var writer = new AsyncBlittableJsonTextWriter(ctx, stream))
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
        public int Shard;
        public Stream AttachmentStream;
        public Stream CommandStream;
        public int PositionInResponse;
    }

    public class ShardedBatchCommand : IEnumerable<SingleShardedCommand>, IDisposable
    {
        private readonly TransactionOperationContext _context;
        private readonly ShardedContext _shardedContext;

        public List<ShardedBatchCommandBuilder.BufferedCommand> BufferedCommands;
        public ArraySegment<BatchRequestParser.CommandData> ParsedCommands;
        public List<Stream> AttachmentStreams;
        public bool IsClusterTransaction;


        internal ShardedBatchCommand(TransactionOperationContext context, ShardedContext shardedContext)
        {
            _context = context;
            _shardedContext = shardedContext;
        }

        public IEnumerator<SingleShardedCommand> GetEnumerator()
        {
            var streamPosition = 0;
            var positionInResponse = 0;
            for (var index = 0; index < ParsedCommands.Count; index++)
            {
                var cmd = ParsedCommands[index];
                var bufferedCommand = BufferedCommands[index];

                if (cmd.Type == CommandType.BatchPATCH)
                {
                    var idsByShard = new Dictionary<int, List<(string Id,string ChangeVector)>>();
                    foreach (var cmdId in cmd.Ids)
                    {
                        if (!(cmdId is BlittableJsonReaderObject bjro))
                            throw new InvalidOperationException();

                        if (bjro.TryGet(nameof(ICommandData.Id), out string id) == false)
                            throw new InvalidOperationException();

                        bjro.TryGet(nameof(ICommandData.ChangeVector), out string expectedChangeVector);

                        var shardId = _shardedContext.GetShardIndex(_context, id);
                        if (idsByShard.TryGetValue(shardId, out var list) == false)
                        {
                            list = new List<(string Id,string ChangeVector)>();
                            idsByShard.Add(shardId, list);
                        }
                        list.Add((id,expectedChangeVector));
                    }

                    foreach (var kvp in idsByShard)
                    {
                        yield return new SingleShardedCommand
                        {
                            Shard = kvp.Key,
                            CommandStream = bufferedCommand.ModifyBatchPatchStream(kvp.Value),
                            PositionInResponse = positionInResponse
                        };
                    }

                    positionInResponse++;
                    continue;
                }

                var shard = _shardedContext.GetShardIndex(_context, cmd.Id);
                var commandStream = bufferedCommand.CommandStream;
                var stream = cmd.Type == CommandType.AttachmentPUT ? AttachmentStreams[streamPosition++] : null;

                if (bufferedCommand.IsIdentity)
                {
                    commandStream = bufferedCommand.ModifyIdentityStream(cmd.Id);
                }

                yield return new SingleShardedCommand
                {
                    Shard = shard,
                    AttachmentStream = stream,
                    CommandStream = commandStream,
                    PositionInResponse = positionInResponse++
                };
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public void Dispose()
        {
        }
    }

    public class ShardedBatchCommandBuilder : BatchRequestParser.BatchCommandBuilder
    {
        public List<Stream> Streams;
        public List<BufferedCommand> BufferedCommands = new List<BufferedCommand>();

        private readonly bool _encrypted;
        private readonly ShardedContext _shardedContext;

        public ShardedBatchCommandBuilder(ShardedRequestHandler handler) :
            base(handler, handler.ShardedContext.DatabaseName, handler.ShardedContext.IdentitySeparator)
        {
            _shardedContext = handler.ShardedContext;
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
            var name = $"{_shardedContext.DatabaseName}.attachment.{Guid.NewGuid():N}.{prefix}";
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
                bufferedCommand.IsIdentity = IsIdentityCommand(ref result);
                BufferedCommands.Add(bufferedCommand);
                return result;
            }
            catch
            {
                await ms.DisposeAsync();
                throw;
            }
        }

        public async Task<ShardedBatchCommand> GetCommand(TransactionOperationContext ctx)
        {
            await ExecuteGetIdentities();
            return new ShardedBatchCommand(ctx, _shardedContext)
            {
                ParsedCommands = Commands, 
                BufferedCommands = BufferedCommands,
                AttachmentStreams = Streams, 
                IsClusterTransaction = IsClusterTransactionRequest
            };
        }

        public class BufferedCommand
        {
            public MemoryStream CommandStream;
            public bool IsIdentity;
            public bool IsBatchPatch;

            // for identities we should replace the id and the change vector
            public int IdStartPosition; 
            public int ChangeVectorPosition; 
            public int IdLength;

            // for batch patch command we need to replace on to the relevant ids
            public int IdsStartPosition; 
            public int IdsEndPosition;

            public MemoryStream ModifyIdentityStream(string newId)
            {
                if (IsIdentity == false)
                    throw new InvalidOperationException("Must be an identity");

                using (CommandStream)
                {
                    var modifier = new IdentityCommandModifier(IdStartPosition, IdLength, ChangeVectorPosition, newId);
                    return modifier.Rewrite(CommandStream);
                }
            }

            public MemoryStream ModifyBatchPatchStream(List<(string Id,string ChangeVector)> list)
            {
                if (IsBatchPatch == false)
                    throw new InvalidOperationException("Must be batch patch");

                var modifier = new PatchCommandModifier(IdsStartPosition, IdsEndPosition - IdsStartPosition, list);
                return modifier.Rewrite(CommandStream);
            }

            public interface IItemModifier
            {
                public void Validate();
                public int GetPosition();
                public int GetLength();
                public byte[] NewValue();
            }

            public class PatchModifier : IItemModifier
            {
                public List<(string Id,string ChangeVector)> List;
                public int IdsStartPosition;
                public int IdsLength;

                public void Validate()
                {
                    if(List == null || List.Count == 0)
                        BufferedCommandModifier.ThrowArgumentMustBePositive("Ids");

                    if(IdsStartPosition <= 0)
                        BufferedCommandModifier.ThrowArgumentMustBePositive("Ids position");

                    if(IdsLength <= 0)
                        BufferedCommandModifier.ThrowArgumentMustBePositive("Ids length");
                }

                public int GetPosition() => IdsStartPosition;

                public int GetLength() => IdsLength;

                public byte[] NewValue()
                {
                    using (var ctx = JsonOperationContext.ShortTermSingleUse())
                    using (var builder = new ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer>(ctx))
                    {
                        builder.Reset(BlittableJsonDocumentBuilder.UsageMode.None);
                        builder.StartArrayDocument();

                        builder.StartWriteArray();
                        foreach (var item in List)
                        {
                            builder.StartWriteObject();
                            builder.WritePropertyName(nameof(ICommandData.Id));
                            builder.WriteValue(item.Id);
                            if (item.ChangeVector != null)
                            {
                                builder.WritePropertyName(nameof(ICommandData.ChangeVector));
                                builder.WriteValue(item.ChangeVector);
                            }
                            builder.WriteObjectEnd();
                        }
                        builder.WriteArrayEnd();
                        builder.FinalizeDocument();

                        var reader = builder.CreateArrayReader();
                        return Encoding.UTF8.GetBytes(reader.ToString());
                    }
                }
            }

            public class ChangeVectorModifier : IItemModifier
            {
                public int ChangeVectorPosition;

                public void Validate()
                {
                    if (ChangeVectorPosition <= 0)
                        BufferedCommandModifier.ThrowArgumentMustBePositive("Change vector position");
                }

                public int GetPosition() => ChangeVectorPosition;
                public int GetLength() => 4; // null
                public byte[] NewValue() => Empty;

                private static readonly byte[] Empty = Encoding.UTF8.GetBytes("\"\"");
            }

            public class IdModifier : IItemModifier
            {
                public int IdStartPosition;
                public int IdLength;

                public string NewId;

                public void Validate()
                {
                    if (IdStartPosition <= 0)
                        BufferedCommandModifier.ThrowArgumentMustBePositive("Id position");

                    if (IdLength <= 0)
                        BufferedCommandModifier.ThrowArgumentMustBePositive("Id length");
                }

                public int GetPosition() => IdStartPosition;
                public int GetLength() => IdLength;
                public byte[] NewValue() => Encoding.UTF8.GetBytes(NewId);
            }

            public class PatchCommandModifier : BufferedCommandModifier
            {
                public PatchCommandModifier(int idsStartPosition, int idsLength, List<(string Id,string ChangeVector)> list)
                {
                    Items = new IItemModifier[1];
                    Items[0] = new PatchModifier
                    {
                        List = list,
                        IdsLength = idsLength,
                        IdsStartPosition = idsStartPosition
                    };
                }
            }

            public class IdentityCommandModifier : BufferedCommandModifier
            {
                public IdentityCommandModifier(int idStartPosition, int idLength, int changeVectorPosition, string newId)
                {
                    Items = new IItemModifier[2];

                    var idModifier = new IdModifier
                    {
                        IdLength = idLength,
                        IdStartPosition = idStartPosition,
                        NewId = newId
                    };
                    var cvModifier = new ChangeVectorModifier
                    {
                        ChangeVectorPosition = changeVectorPosition
                    };

                    if (changeVectorPosition < idStartPosition)
                    {
                        Items[0] = cvModifier;
                        Items[1] = idModifier;
                    }
                    else
                    {
                        Items[1] = cvModifier;
                        Items[0] = idModifier;
                    }
                }
            }

            public abstract class BufferedCommandModifier
            {
                protected IItemModifier[] Items;
               

                public MemoryStream Rewrite(MemoryStream source)
                {
                    EnsureInitialized();
                    
                    var offset = 0;
                    var dest = new MemoryStream();
                    try
                    {
                        source.Position = 0;

                        var sourceBuffer = source.GetBuffer();

                        foreach (var item in Items)
                        {
                            offset = WriteRemaining(item.GetPosition());
                            dest.Write(item.NewValue());
                            offset += item.GetLength();
                        }
                
                        // copy the rest
                        source.Position = offset;
                        source.CopyTo(dest);

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
                    catch
                    {
                        dest.Dispose();
                        throw;
                    }

                    return dest;
                }

                private void EnsureInitialized()
                {
                    if (Items == null || Items.Length == 0)
                        throw new InvalidOperationException();

                    foreach (var item in Items)
                    {
                        item.Validate();
                    }
                }

                public static void ThrowArgumentMustBePositive(string name)
                {
                    throw new ArgumentException($"{name} must be positive");
                }
            }
        }
    }
}

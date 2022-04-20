using System;
using System.Buffers;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Server.Documents.Handlers.Batches;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Utils;

namespace Raven.Server.Documents.Handlers.BulkInsert;

public abstract class AbstractBulkInsertBatchCommandsReader<TCommandData> : IDisposable where TCommandData : IBatchCommandData
{
    private readonly Stream _stream;
    private readonly UnmanagedJsonParser _parser;
    private readonly JsonOperationContext.MemoryBuffer _buffer;
    protected readonly BatchRequestParser BatchRequestParser;
    private readonly JsonParserState _state;
    private readonly CancellationToken _token;

    protected AbstractBulkInsertBatchCommandsReader(JsonOperationContext ctx, Stream stream, JsonOperationContext.MemoryBuffer buffer, BatchRequestParser batchRequestParser, CancellationToken token)
    {
        _stream = stream;
        _buffer = buffer;
        BatchRequestParser = batchRequestParser;
        _token = token;

        _state = new JsonParserState();
        _parser = new UnmanagedJsonParser(ctx, _state, "bulk_docs");
    }

    public async Task InitAsync()
    {
        while (_parser.Read() == false)
            await BatchRequestParser.RefillParserBuffer(_stream, _buffer, _parser, _token);

        if (_state.CurrentTokenType != JsonParserToken.StartArray)
        {
            BatchRequestParser.ThrowUnexpectedToken(JsonParserToken.StartArray, _state);
        }
    }

    public abstract Task<TCommandData> GetCommandAsync(JsonOperationContext ctx, BlittableMetadataModifier modifier);

    protected Task<BatchRequestParser.CommandData> MoveNextAsync(JsonOperationContext ctx, BlittableMetadataModifier modifier)
    {
        if (_parser.Read())
        {
            if (_state.CurrentTokenType == JsonParserToken.EndArray)
                return null;
            return BatchRequestParser.ReadSingleCommand(ctx, _stream, _state, _parser, _buffer, modifier, _token);
        }

        return MoveNextUnlikely(ctx, modifier);
    }

    private async Task<BatchRequestParser.CommandData> MoveNextUnlikely(JsonOperationContext ctx, BlittableMetadataModifier modifier)
    {
        do
        {
            await BatchRequestParser.RefillParserBuffer(_stream, _buffer, _parser, _token);
        } while (_parser.Read() == false);

        if (_state.CurrentTokenType == JsonParserToken.EndArray)
            return new BatchRequestParser.CommandData { Type = CommandType.None };

        return await BatchRequestParser.ReadSingleCommand(ctx, _stream, _state, _parser, _buffer, modifier, _token);
    }

    public Stream GetBlob(long blobSize)
    {
        var bufferSize = _parser.BufferSize - _parser.BufferOffset;
        var copy = ArrayPool<byte>.Shared.Rent(bufferSize);
        var copySpan = new Span<byte>(copy);

        _buffer.Memory.Memory.Span.Slice(_parser.BufferOffset, bufferSize).CopyTo(copySpan);

        _parser.Skip(blobSize < bufferSize ? (int)blobSize : bufferSize);

        return new LimitedStream(new ConcatStream(new ConcatStream.RentedBuffer
        {
            Buffer = copy,
            Count = bufferSize,
            Offset = 0
        }, _stream), blobSize, 0, 0);
    }

    public virtual void Dispose()
    {
        _parser.Dispose();
    }
}

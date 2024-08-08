using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Sharding.Streaming;

public sealed class ReadContinuationState : IDisposable
{
    private StreamResult _response;
    private readonly CancellationToken _token;
    private JsonOperationContext _inputContext;
    private JsonParserState _state;
    private UnmanagedJsonParser _parser;
    private JsonOperationContext.MemoryBuffer _buffer;
    private JsonOperationContext.MemoryBuffer.ReturnBuffer _returnBuffer;
    private PeepingTomStream _peepingTomStream;
    private JsonOperationContext _builderContext;
    private IDisposable _builderReturnContext;
    private IDisposable _inputReturnContext;
    private BlittableJsonDocumentBuilder _builder;

    private int _maxCachedLimit = 1024;

    public ReadContinuationState(JsonContextPool pool, StreamResult response, CancellationToken token)
    {
        _response = response ?? throw new InvalidOperationException("The index does not exists, failed to stream results");
        _token = token;
        _builderReturnContext = pool.AllocateOperationContext(out _builderContext);
        _inputReturnContext = pool.AllocateOperationContext(out _inputContext);

        _peepingTomStream = new PeepingTomStream(_response.Stream, _inputContext);

        _state = new JsonParserState();
        _parser = new UnmanagedJsonParser(_inputContext, _state, "stream contents");
        _builder = new BlittableJsonDocumentBuilder(_builderContext, BlittableJsonDocumentBuilder.UsageMode.ToDisk, "readArray/singleResult", _parser,
            _state);
        _returnBuffer = _inputContext.GetMemoryBuffer(out _buffer);
    }

    public async ValueTask InitializeAsync()
    {
        if (await ReadAsync() == false)
            ThrowInvalidJson();

        if (CurrentTokenType != JsonParserToken.StartObject)
            ThrowInvalidJson();
    }

    public string ReadString() => UnmanagedJsonParserHelper.ReadString(_builderContext, _peepingTomStream, _parser, _state, _buffer);

    public Task<bool> ReadAsync() => UnmanagedJsonParserHelper.ReadAsync(_peepingTomStream, _parser, _state, _buffer, _token);

    public async ValueTask<BlittableJsonReaderArray> ReadJsonArrayAsync()
    {
        _builder.Renew("readArray", BlittableJsonDocumentBuilder.UsageMode.ToDisk);

        var result = await UnmanagedJsonParserHelper.ReadJsonArrayAsync(_builder, _peepingTomStream, _parser, _buffer, _token);

        _builder.Reset();

        return result;
    }

    public async ValueTask<BlittableJsonReaderObject> ReadObjectAsync()
    {
        _builder.Renew("readArray/singleResult", BlittableJsonDocumentBuilder.UsageMode.ToDisk);

        await UnmanagedJsonParserHelper.ReadObjectAsync(_builder, _peepingTomStream, _parser, _buffer, _token);

        var result = _builder.CreateReader();

        _builder.Reset();

        return result;
    }

    public JsonParserToken CurrentTokenType => _state.CurrentTokenType;
    public long ReadLong => _state.Long;

    public void ThrowInvalidJson() => UnmanagedJsonParserHelper.ThrowInvalidJson(_peepingTomStream);

    public bool TryRenewCacheIfNeeded(int limit)
    {
        if (_builderContext.AllocatedMemory > 4 * 1024 * 1024 || limit > _maxCachedLimit)
        {
            _builderContext.Reset();
            _builderContext.Renew();
            return true;
        }

        if (_builderContext.CachedProperties.NeedClearPropertiesCache() == false) 
            return false;
        
        _builderContext.CachedProperties.Reset();
        return true;
    }

    public void Dispose()
    {
        _response.Response.Dispose();
        _response.Stream.Dispose();
        _parser.Dispose();
        _returnBuffer.Dispose();
        _peepingTomStream.Dispose();
        _builder.Dispose();
        _builderReturnContext.Dispose();
        _inputReturnContext?.Dispose();
    }
}

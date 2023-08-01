using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Sharding.Streaming;

public sealed class YieldShardStreamResults : IAsyncEnumerator<BlittableJsonReaderObject>
{
    private readonly ReadContinuationState _readingState;
    private readonly string _arrayPropertyName;
    private bool _initialized;
    private int _processed;

    public YieldShardStreamResults(ReadContinuationState readingState, string arrayPropertyName)
    {
        _readingState = readingState;
        _arrayPropertyName = arrayPropertyName;
    }

    public async ValueTask<bool> MoveNextAsync()
    {
        AssertInitialized();

        if (_readingState.TryRenewCacheIfNeeded(_processed++))
            _processed = 0;

        if (await _readingState.ReadAsync() == false)
            _readingState.ThrowInvalidJson();

        if (_readingState.CurrentTokenType is JsonParserToken.EndArray or JsonParserToken.EndObject)
            return false;

        Current = await _readingState.ReadObjectAsync();
        return true;
    }

    public async ValueTask StartReadArray()
    {
        if (await _readingState.ReadAsync() == false)
            _readingState.ThrowInvalidJson();

        if (_readingState.CurrentTokenType != JsonParserToken.StartArray)
            _readingState.ThrowInvalidJson();
    }

    public async Task InitializeAsync()
    {
        _initialized = true;

        var name = _readingState.ReadString();

        if (name != _arrayPropertyName)
            _readingState.ThrowInvalidJson();

        await StartReadArray();
    }

    public void Reset()
    {
        throw new NotSupportedException("Enumerator does not support resetting");
    }

    public BlittableJsonReaderObject Current { get; private set; }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void AssertInitialized()
    {
        if (_initialized == false)
            throw new InvalidOperationException("Enumerator is not initialized. Please initialize it first.");
    }

    public async ValueTask DisposeAsync()
    {
        // need to read to end of the array
        while (await MoveNextAsync())
        {

        }
    }
}

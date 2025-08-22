#nullable enable

using System;
using System.Threading;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.AI;

/// <summary>
/// This is meant to allow us to parse partial JSON from LLM models.
///
/// We use the parser to build the JSON in an incremental, as well as
/// to be able to stream a property from the JSON "as it is being parsed"
/// to the caller. 
/// </summary>
public unsafe class SseStreamingJsonParser : IDisposable
{
    private readonly JsonOperationContext _context;
    private readonly UnmanagedJsonParser _parser;
    private readonly BlittableJsonDocumentBuilder _builder;

    private long _totalSize;
    private readonly int _maxSize;

    public SseStreamingJsonParser(JsonOperationContext context, string property, int maxSize = int.MaxValue)
    {
        _context = context;
        _maxSize = maxSize;

        var jsonParserState = new JsonParserState();
        _parser = new UnmanagedJsonParser(context, jsonParserState, "streaming/parsing");
        _builder = new BlittableJsonDocumentBuilder(context, BlittableJsonDocumentBuilder.UsageMode.ForStreaming, "streaming/parsing", _parser, jsonParserState);
        _builder.PropertyToWatchForStreaming = (property, OnStringReadInvoke);
        _context.CachedProperties.NewDocument();
        _builder.ReadObjectDocument();
    }

    public event Action<UnmanagedWriteBuffer>? OnStringRead;

    private void OnStringReadInvoke(object? sender, UnmanagedWriteBuffer e)
    {
        OnStringRead?.Invoke(e);
    }

    public void Reset()
    {
        _context.CachedProperties.NewDocument();
        _builder.ReadObjectDocument();
    }

    public BlittableJsonReaderObject? Process(LazyStringValue dataChunk, CancellationToken? token = null)
    {
        token?.ThrowIfCancellationRequested();

        _totalSize += dataChunk.Length;
        if (_totalSize > _maxSize)
            throw new ArgumentException($"The maximum size allowed ({_maxSize}) has been exceeded, aborting");

        _parser.SetBuffer(dataChunk.Buffer, dataChunk.Length);
        if (_builder.Read())
        {
            _builder.FinalizeDocument();
            return _builder.CreateReader();
        }

        return null;
    }

    public void Dispose()
    {
        _parser?.Dispose();
        _builder?.Dispose();
    }
}

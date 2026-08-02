#nullable enable

using System;
using System.IO;
using System.Threading;
using Sparrow.Exceptions;
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

    private void OnStringReadInvoke(UnmanagedWriteBuffer e)
    {
        OnStringRead?.Invoke(e);
    }

    public void Reset()
    {
        IsInvalid = false;
        _totalSize = 0;
        _context.CachedProperties.NewDocument();
        _builder.ReadObjectDocument();
    }

    /// <summary>
    /// Set once the model's streamed content was rejected as JSON. Latched: the parser is not fed again after
    /// that, so the caller can defer the failure to the end of the stream and still observe the terminating
    /// finish_reason.
    /// </summary>
    public bool IsInvalid { get; private set; }

    /// <summary>
    /// Non-throwing <see cref="Process"/>. Malformed JSON and a non-object root (both of which mean the model
    /// did not produce the requested JSON) latch <see cref="IsInvalid"/> and return false; any other failure
    /// propagates. Returns false immediately once <see cref="IsInvalid"/> is set.
    /// </summary>
    public bool TryProcess(LazyStringValue dataChunk, out BlittableJsonReaderObject? result, CancellationToken? token = null)
    {
        result = null;

        if (IsInvalid)
            return false;

        try
        {
            result = Process(dataChunk, token);
            return true;
        }
        catch (Exception e) when (e is InvalidDataException or InvalidStartOfObjectException)
        {
            IsInvalid = true;
            return false;
        }
    }

    public BlittableJsonReaderObject? Process(LazyStringValue dataChunk, CancellationToken? token = null)
    {
        token?.ThrowIfCancellationRequested();

        _totalSize += dataChunk.Size;
        if (_totalSize > _maxSize)
            throw new ArgumentException($"The maximum size allowed ({_maxSize}) has been exceeded, aborting");

        _parser.SetBuffer(dataChunk.Buffer, dataChunk.Size);
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

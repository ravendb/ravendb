using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Streaming;

public sealed class StreamResultsWriter : IStreamResultsWriter<Document>
{
    private readonly AsyncBlittableJsonTextWriter _writer;
    private readonly JsonOperationContext _context;
    private bool _first = true;
    

    public StreamResultsWriter(Stream stream, JsonOperationContext context, CancellationToken token)
    {
        _context = context;
        _writer = new AsyncBlittableJsonTextWriter(context, stream, token);
    }
    
    public ValueTask DisposeAsync()
    {
        return _writer.DisposeAsync();
    }

    public void StartResponse()
    {
        _writer.WriteStartObject();
    }

    public void StartResults()
    {
        _writer.WritePropertyName("Results");
        _writer.WriteStartArray();
    }

    public void EndResults()
    {
        _writer.WriteEndArray();
    }

    public async ValueTask AddResultAsync(Document res, CancellationToken token)
    {
        if (_first == false)
        {
            _writer.WriteComma();
        }
        else
        {
            _first = false;
        }
        
        _writer.WriteDocument(_context, res, metadataOnly: false);
        await _writer.MaybeFlushAsync(token);
    }

    public void EndResponse()
    {
        _writer.WriteEndObject();
    }
}

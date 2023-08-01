using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Streaming;

public sealed class StreamJsonlResultsWriter : IStreamResultsWriter<Document>
{
    private readonly AsyncBlittableJsonTextWriter _writer;
    private readonly JsonOperationContext _context;

    public StreamJsonlResultsWriter(Stream stream, JsonOperationContext context, CancellationToken token)
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
    }

    public void StartResults()
    {
    }

    public void EndResults()
    {
    }
    
    public async ValueTask AddResultAsync(Document res, CancellationToken token)
    {
        _writer.WriteDocument(_context, res, metadataOnly: false);
        _writer.WriteNewLine();
        await _writer.MaybeFlushAsync(token);
    }

    public void EndResponse()
    {
    }
}

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Raven.Server.Documents;
using Raven.Server.Smuggler.Documents.Data;
using Sparrow.Json;
using Voron;

namespace Raven.Server.Smuggler.Documents;

public class OrchestratorStreamSource : StreamSource
{
    public OrchestratorStreamSource(Stream stream, JsonOperationContext context, string databaseName, DatabaseSmugglerOptionsServerSide options = null) : base(stream, context, databaseName, options)
    {
            
    }

    private Dictionary<Slice, StreamsTempFile.InnerStream> _uniqueStreams = new Dictionary<Slice, StreamsTempFile.InnerStream>(SliceComparer.Instance);

    public override Stream GetAttachmentStream(LazyStringValue hash, out string tag)
    {
        using (Slice.From(_allocator, hash, out var slice))
        {
            if (_uniqueStreams.TryGetValue(slice, out var stream))
            {
                tag = "$from-sharding-import";
                return stream.CreateDisposableReaderStream();
            }
        }
        tag = null;
        return null;
    }

    public override async Task<DocumentItem.AttachmentStream> ProcessAttachmentStreamAsync(JsonOperationContext context, BlittableJsonReaderObject data, INewDocumentActions actions)
    {
        var r = await base.ProcessAttachmentStreamAsync(context, data, actions);
        if (r.Stream is StreamsTempFile.InnerStream inner == false)
            throw new InvalidOperationException();

        if (_uniqueStreams.ContainsKey(r.Base64Hash) == false)
        {
            var hash = r.Base64Hash.Clone(_allocator);
            _uniqueStreams.Add(hash, inner);
        }
            
        return r;
    }

    public override void Dispose()
    {
        foreach (var (_, stream) in _uniqueStreams)
        {
            using (stream)
            {
                
            }
        }

        // here we release the allocator
        base.Dispose();
    }
}

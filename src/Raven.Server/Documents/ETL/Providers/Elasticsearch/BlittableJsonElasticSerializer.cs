using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Transport;
using Raven.Client.Util;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Sync;

namespace Raven.Server.Documents.ETL.Providers.ElasticSearch;

internal class BlittableJsonElasticSerializer : Serializer
{
    private DocumentsOperationContext _context;

    public DisposableAction SetContext(DocumentsOperationContext context)
    {
        _context = context;
        return new DisposableAction(() => _context = null);
    }
    
    public override void Serialize<T>(T data, Stream stream, SerializationFormatting formatting = SerializationFormatting.None)
    {
        if (_context is null)
        {
            throw new InvalidOperationException("Context cannot be null");
        }
        if (data is not BlittableJsonReaderObject json)
        {
            throw new NotSupportedException(
                $"Blittable elastic serializer cannot serialize object of type '{data.GetType()}'. Object type needs to be '{typeof(BlittableJsonReaderObject)}'");
        }
        
        using (var writer = new BlittableJsonTextWriter(_context, stream))
        {
            writer.WriteObject(json);
        }
    }

    public override Task SerializeAsync<T>(T data, Stream stream,
        SerializationFormatting formatting = SerializationFormatting.None, CancellationToken cancellationToken = default) =>
        throw new NotSupportedException();
        
    public override object Deserialize(Type type, Stream stream) =>
        throw new NotSupportedException();
    
    public override T Deserialize<T>(Stream stream) =>
        throw new NotSupportedException();

    public override ValueTask<object> DeserializeAsync(Type type, Stream stream, CancellationToken cancellationToken = default) =>
        throw new NotSupportedException();

    public override ValueTask<T> DeserializeAsync<T>(Stream stream, CancellationToken cancellationToken = default) =>
        throw new NotSupportedException();
}

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Transport;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Sync;

namespace Raven.Server.Documents.ETL.Providers.ElasticSearch;

internal class BlittableJsonElasticSerializer : Serializer, IDisposable
{
    private DocumentsOperationContext Context { get; set; }

    public BlittableJsonElasticSerializer SetContext(DocumentsOperationContext context)
    {
        Context = context;
        return this;
    }
    
    public void Dispose()
    {
        Context = null;
    }

    public override void Serialize<T>(T data, Stream stream, SerializationFormatting formatting = SerializationFormatting.None)
    {
        if (Context is null)
        {
            throw new InvalidOperationException("Context cannot be null");
        }
        if (data is not BlittableJsonReaderObject json)
        {
            throw new NotSupportedException();
        }
        
        using (var writer = new BlittableJsonTextWriter(Context, stream))
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

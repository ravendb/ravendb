using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Transport;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Sync;

namespace Raven.Server.Documents.ETL.Providers.ElasticSearch;

internal class BlittableJsonElasticSerializer : Serializer
{
    public DocumentsOperationContext Context { private get; set; }

    public override void Serialize<T>(T data, Stream stream, SerializationFormatting formatting = SerializationFormatting.None)
    {
        if (data is not BlittableJsonReaderObject json)
        {
            throw new NotImplementedException();
        }
        
        using (var writer = new BlittableJsonTextWriter(Context, stream))
        {
            writer.WriteObject(json);
        }
    }

    public override Task SerializeAsync<T>(T data, Stream stream,
        SerializationFormatting formatting = SerializationFormatting.None, CancellationToken cancellationToken = default) =>
        throw new NotImplementedException();
        
    public override object Deserialize(Type type, Stream stream) =>
        throw new NotImplementedException();
    
    public override T Deserialize<T>(Stream stream) =>
        throw new NotImplementedException();

    public override ValueTask<object> DeserializeAsync(Type type, Stream stream, CancellationToken cancellationToken = default) =>
        throw new NotImplementedException();

    public override ValueTask<T> DeserializeAsync<T>(Stream stream, CancellationToken cancellationToken = default) =>
        throw new NotImplementedException();
}

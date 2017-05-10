using System;
using Raven.Client.Documents.Conventions;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Commands.Batches
{
    internal class PutCommandDataWithBlittableJson : PutCommandDataBase<BlittableJsonReaderObject>
    {
        public PutCommandDataWithBlittableJson(string id, long? etag, BlittableJsonReaderObject document)
            : base(id, etag, document)
        {
        }
    }

    public class PutCommandData : PutCommandDataBase<DynamicJsonValue>
    {
        public PutCommandData(string id, long? etag, DynamicJsonValue document)
            : base(id, etag, document)
        {
        }
    }

    public abstract class PutCommandDataBase<T> : ICommandData
    {
        protected PutCommandDataBase(string id, long? etag, T document)
        {
            if (document == null)
                throw new ArgumentNullException(nameof(document));

            Key = id;
            Etag = etag;
            Document = document;
        }

        public string Key { get; }
        public long? Etag { get; }
        public T Document { get; }
        public CommandType Type { get; } = CommandType.PUT;

        public DynamicJsonValue ToJson(DocumentConventions conventions, JsonOperationContext context)
        {
            return new DynamicJsonValue
            {
                [nameof(Key)] = Key,
                [nameof(Etag)] = Etag,
                [nameof(Document)] = Document,
                [nameof(Type)] = Type.ToString()
            };
        }
    }
}
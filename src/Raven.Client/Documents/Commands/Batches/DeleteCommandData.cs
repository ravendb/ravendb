using System;
using Raven.Client.Documents.Conventions;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Commands.Batches
{
    public class DeleteCommandData : ICommandData
    {
        public DeleteCommandData(string id, long? etag)
        {
            Key = id ?? throw new ArgumentNullException(nameof(id));
            Etag = etag;
        }

        public string Key { get; }
        public long? Etag { get; }
        public CommandType Type { get; } = CommandType.DELETE;

        public virtual DynamicJsonValue ToJson(DocumentConventions conventions, JsonOperationContext context)
        {
            return new DynamicJsonValue
            {
                [nameof(Key)] = Key,
                [nameof(Etag)] = Etag,
                [nameof(Type)] = Type.ToString()
            };
        }
    }
}
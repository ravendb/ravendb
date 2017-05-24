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
            Id = id ?? throw new ArgumentNullException(nameof(id));
            Etag = etag;
        }

        public string Id { get; }
        public long? Etag { get; }
        public CommandType Type { get; } = CommandType.DELETE;

        public virtual DynamicJsonValue ToJson(DocumentConventions conventions, JsonOperationContext context)
        {
            return new DynamicJsonValue
            {
                [nameof(Id)] = Id,
                [nameof(Etag)] = Etag,
                [nameof(Type)] = Type.ToString()
            };
        }
    }
}
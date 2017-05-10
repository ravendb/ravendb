using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Commands.Batches
{
    public class DeleteCommandData : ICommandData
    {
        public DeleteCommandData(string id, long? etag)
        {
            if (id == null)
                throw new ArgumentNullException(nameof(id));

            Key = id;
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
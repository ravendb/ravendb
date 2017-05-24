using System;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Commands.Batches
{
    public class PatchCommandData : ICommandData
    {
        public PatchCommandData(string id, long? etag, PatchRequest patch, PatchRequest patchIfMissing)
        {
            Id = id ?? throw new ArgumentNullException(nameof(id));
            Etag = etag;
            Patch = patch ?? throw new ArgumentNullException(nameof(patch));
            PatchIfMissing = patchIfMissing;
        }

        public string Id { get; }
        public long? Etag { get; }
        public PatchRequest Patch { get; }
        public PatchRequest PatchIfMissing { get; }
        public CommandType Type { get; } = CommandType.PATCH;

        public DynamicJsonValue ToJson(DocumentConventions conventions, JsonOperationContext context)
        {
            var json = new DynamicJsonValue
            {
                [nameof(Id)] = Id,
                [nameof(Etag)] = Etag,
                [nameof(Patch)] = Patch.ToJson(conventions, context),
                [nameof(Type)] = Type.ToString()
            };

            if (PatchIfMissing != null)
                json[nameof(PatchIfMissing)] = PatchIfMissing?.ToJson(conventions, context);

            return json;
        }
    }
}
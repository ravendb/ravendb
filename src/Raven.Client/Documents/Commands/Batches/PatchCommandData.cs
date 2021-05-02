using System;
using System.Collections.Generic;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Session;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Commands.Batches
{
    public class PatchCommandData : ICommandData
    {
        public PatchCommandData()
        {
            
        }

        public PatchCommandData(string id, string changeVector, PatchRequest patch, PatchRequest patchIfMissing = null)
        {
            Id = id ?? throw new ArgumentNullException(nameof(id));
            ChangeVector = changeVector;
            Patch = patch ?? throw new ArgumentNullException(nameof(patch));
            PatchIfMissing = patchIfMissing;
        }
        public string Id { get; }
        public string Name { get; } = null;
        public BlittableJsonReaderObject CreateIfMissing { get; set; }
        public string ChangeVector { get; }
        public PatchRequest Patch { get; }
        public PatchRequest PatchIfMissing { get; }
        public CommandType Type { get; } = CommandType.PATCH;
        public bool ReturnDocument { get; private set; }

        public DynamicJsonValue ToJson(DocumentConventions conventions, JsonOperationContext context)
        {
            var json = new DynamicJsonValue
            {
                [nameof(Id)] = Id,
                [nameof(ChangeVector)] = ChangeVector,
                [nameof(Patch)] = Patch.ToJson(conventions, context),
                [nameof(Type)] = Type.ToString()
            };

            if (PatchIfMissing != null)
                json[nameof(PatchIfMissing)] = PatchIfMissing.ToJson(conventions, context);

            if (CreateIfMissing != null)
                json[nameof(CreateIfMissing)] = CreateIfMissing;

            if (ReturnDocument)
                json[nameof(ReturnDocument)] = ReturnDocument;

            return json;
        }

        public void OnBeforeSaveChanges(InMemoryDocumentSessionOperations session)
        {
            ReturnDocument = session.IsLoaded(Id);
        }
    }
}

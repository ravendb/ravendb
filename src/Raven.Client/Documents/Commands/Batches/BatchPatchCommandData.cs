using System;
using System.Collections.Generic;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Session;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Commands.Batches
{
    /// <summary>
    /// Commands that patches multiple documents using same patch script
    /// CAUTION: This command does not update session state after .SaveChanges() call 
    /// </summary>
    public class BatchPatchCommandData : ICommandData
    {
        private readonly HashSet<string> _seenIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        private readonly List<(string Id, string ChangeVector)> _ids = new List<(string Id, string ChangeVector)>();

        private BatchPatchCommandData(PatchRequest patch, PatchRequest patchIfMissing)
        {
            Patch = patch ?? throw new ArgumentNullException(nameof(patch));
            PatchIfMissing = patchIfMissing;
        }

        public BatchPatchCommandData(List<string> ids, PatchRequest patch, PatchRequest patchIfMissing)
            : this(patch, patchIfMissing)
        {
            if (ids == null)
                throw new ArgumentNullException(nameof(ids));
            if (ids.Count == 0)
                throw new ArgumentException("Value cannot be an empty collection.", nameof(ids));

            foreach (var id in ids)
                Add(id);
        }

        public BatchPatchCommandData(List<(string Id, string ChangeVector)> ids, PatchRequest patch, PatchRequest patchIfMissing)
            : this(patch, patchIfMissing)
        {
            if (ids == null)
                throw new ArgumentNullException(nameof(ids));
            if (ids.Count == 0)
                throw new ArgumentException("Value cannot be an empty collection.", nameof(ids));

            foreach (var kvp in ids)
                Add(kvp.Id, kvp.ChangeVector);
        }

        private void Add(string id, string changeVector = null)
        {
            if (string.IsNullOrWhiteSpace(id))
                throw new ArgumentException("Value cannot be null or whitespace.", nameof(id));

            if (_seenIds.Add(id) == false)
                throw new InvalidOperationException($"Could not add ID '{id}' because item with the same ID was already added");

            _ids.Add((id, changeVector));
        }

        public IReadOnlyList<(string Id, string ChangeVector)> Ids => _ids;

        public string Id => throw new NotSupportedException();

        public string Name { get; } = null;

        public PatchRequest Patch { get; }

        public PatchRequest PatchIfMissing { get; }

        public string ChangeVector => throw new NotSupportedException();

        public CommandType Type => CommandType.BatchPATCH;

        public DynamicJsonValue ToJson(DocumentConventions conventions, JsonOperationContext context)
        {
            var ids = new DynamicJsonArray();
            foreach (var kvp in _ids)
            {
                var id = new DynamicJsonValue
                {
                    [nameof(kvp.Id)] = kvp.Id
                };

                if (kvp.ChangeVector != null)
                    id[nameof(kvp.ChangeVector)] = kvp.ChangeVector;

                ids.Add(id);
            }

            var json = new DynamicJsonValue
            {
                [nameof(Ids)] = ids,
                [nameof(Patch)] = Patch.ToJson(conventions, context),
                [nameof(Type)] = Type.ToString()
            };

            if (PatchIfMissing != null)
                json[nameof(PatchIfMissing)] = PatchIfMissing.ToJson(conventions, context);

            return json;
        }

        public void OnBeforeSaveChanges(InMemoryDocumentSessionOperations session)
        {
            // this command does not update session state after SaveChanges call!
        }
    }
}

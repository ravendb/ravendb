using System;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Commands.Batches
{
    public sealed class DeleteAttachmentCommandData : ICommandData
    {
        public DeleteAttachmentCommandData(string documentId, string name, string changeVector)
            : this(documentId, name, changeVector, storageOnly: false, fromEtl: false, AttachmentFlags.None)
        {
        }

        public DeleteAttachmentCommandData(string documentId, string name, bool storageOnly)
            : this(documentId, name, changeVector: null, storageOnly, fromEtl: false, AttachmentFlags.Retired)
        {
        }

        internal DeleteAttachmentCommandData(string documentId, string name, string changeVector, bool storageOnly, bool fromEtl, AttachmentFlags flags)
        {
            if (string.IsNullOrWhiteSpace(documentId))
                throw new ArgumentNullException(nameof(documentId));
            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentNullException(nameof(name));

            Id = documentId;
            Name = name;
            ChangeVector = changeVector;
            StorageOnly = storageOnly;
            FromEtl = fromEtl;
            Flags = flags;
        }

        public string Id { get; }
        public string Name { get; }
        public string ChangeVector { get; }
        public CommandType Type { get => CommandType.AttachmentDELETE; }
        public bool StorageOnly { get; }
        internal bool FromEtl { get; }
        internal AttachmentFlags Flags { get; }

        public DynamicJsonValue ToJson(DocumentConventions conventions, JsonOperationContext context)
        {
            return new DynamicJsonValue
            {
                [nameof(Id)] = Id,
                [nameof(Name)] = Name,
                [nameof(ChangeVector)] = ChangeVector,
                [nameof(Type)] = Type.ToString(),
                [nameof(Flags)] = Flags.ToString(),
                [nameof(StorageOnly)] = StorageOnly,
                [nameof(FromEtl)] = FromEtl,
            };
        }

        public void OnBeforeSaveChanges(InMemoryDocumentSessionOperations session)
        {
        }
    }
}

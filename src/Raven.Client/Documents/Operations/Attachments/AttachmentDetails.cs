using System;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Attachments
{
    public class AttachmentDetails : AttachmentName
    {
        public string ChangeVector;
        public string DocumentId;
    }

    internal class AttachmentNameWithCount : AttachmentName
    {
        public long Count { get; set; }

        internal override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(Count)] = Count;

            return json;
        }
    }

    public class AttachmentName
    {
        public string Name;
        public string Hash;
        public string ContentType;
        public long Size;

        internal virtual DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Name)] = Name,
                [nameof(Hash)] = Hash,
                [nameof(ContentType)] = ContentType,
                [nameof(Size)] = Size
            };
        }
    }

    public class AttachmentRequest
    {
        public AttachmentRequest(string documentId, string name)
        {
            if (string.IsNullOrWhiteSpace(documentId))
                throw new ArgumentNullException($"{nameof(documentId)} cannot be null or whitespace.");

            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentNullException($"{nameof(name)} cannot be null or whitespace.");

            DocumentId = documentId;
            Name = name;
        }

        public string Name { get; }
        public string DocumentId { get; }
    }
}

using System;
using Raven.Client.Documents.Attachments;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Attachments
{
    public class AttachmentDetails : AttachmentName
    {
        public string ChangeVector;
        public string DocumentId;
    }

    internal sealed class AttachmentNameWithCount : AttachmentName
    {
        public long Count { get; set; }
        public long RetiredCount { get; set; }
        public long TotalCount { get; set; }

        internal override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(Count)] = Count;
            json[nameof(RetiredCount)] = RetiredCount;
            json[nameof(TotalCount)] = TotalCount;

            return json;
        }
    }

    public class AttachmentName
    {
        public string Name;
        public string Hash;
        public string ContentType;
        public long Size;
        public AttachmentFlags Flags;
        public DateTime? RetireAt;
        internal virtual DynamicJsonValue ToJson()
        {
            var json = new DynamicJsonValue
            {
                [nameof(Name)] = Name,
                [nameof(Hash)] = Hash,
                [nameof(ContentType)] = ContentType,
                [nameof(Size)] = Size
            };
            json[nameof(Flags)] = Flags.ToString();
            json[nameof(RetireAt)] = RetireAt;

            return json;
        }
    }

    public sealed class AttachmentRequest
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

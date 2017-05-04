using Raven.Server.Documents;

namespace Raven.Server.Smuggler.Documents
{
    public enum DocumentType : byte
    {
        Document = 1,
        Attachment = 2,
    }

    public struct DocumentItem
    {
        public const string Key = "@document-type";

        public DocumentType Type;
        public Document Document;
        public StreamSource.AttachmentStream Attachment;
    }
}
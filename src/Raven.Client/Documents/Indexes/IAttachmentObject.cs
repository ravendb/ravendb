using System.Text;

namespace Raven.Client.Documents.Indexes
{
    public interface IAttachmentObject
    {
        public string Name { get; }

        public string Hash { get; }

        public string ContentType { get; }

        public long Size { get; }

        public string GetContentAsString();

        public string GetContentAsString(Encoding encoding);
    }
}

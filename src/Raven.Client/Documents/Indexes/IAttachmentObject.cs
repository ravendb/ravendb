using System;
using System.IO;
using System.Text;
using Raven.Client.Documents.Attachments;

namespace Raven.Client.Documents.Indexes
{
    public interface IAttachmentObject
    {
        public DateTime? RemoteAt { get; }

        public RemoteAttachmentFlags RemoteFlags { get; }

        public string RemoteIdentifier { get; }

        public string Name { get; }

        public string Hash { get; }

        public string ContentType { get; }

        public long Size { get; }

        public string GetContentAsString();

        public string GetContentAsString(Encoding encoding);

        public Stream GetContentAsStream();
    }
}

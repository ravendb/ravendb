using System;
using System.IO;

namespace Raven.Client.Documents.Operations.Attachments
{
    public class AttachmentResult : IDisposable
    {
        public Stream Stream;
        public AttachmentDetails Details;

        public void Dispose()
        {
            Stream?.Dispose();
            Stream = null;
        }
    }
}
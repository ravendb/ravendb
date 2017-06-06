using System;
using System.IO;
using System.Net.Http;

namespace Raven.Client.Documents.Operations
{
    public class AttachmentResultWithStream : AttachmentResult, IDisposable
    {
        private HttpResponseMessage _response;

        public Stream Stream;

        public AttachmentResultWithStream(HttpResponseMessage response)
        {
            _response = response;
        }

        public void Dispose()
        {
            Stream?.Dispose();
            Stream = null;
            _response?.Dispose();
            _response = null;
        }
    }

    public class AttachmentResult : AttachmentName
    {
        public long Etag;
        public string DocumentId;
    }

    public class AttachmentName
    {
        public string Name;
        public string Hash;
        public string ContentType;
        public long Size;
    }
}
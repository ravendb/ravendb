using System;
using System.IO;
using System.Threading.Tasks;

namespace Raven.Client.Documents.Operations.Attachments
{
    public class AttachmentEnumeratorResult : IDisposable
    {
        private AttachmentsStream _internalStream;

        internal AttachmentEnumeratorResult(AttachmentsStream internalStream)
        {
            _internalStream = internalStream;
        }

        public AttachmentDetails Details;

        public void CopyTo(Stream stream)
        {
            _internalStream.CopyTo(stream);
        }

        public Task CopyToAsync(Stream stream)
        {
            return _internalStream.CopyToAsync(stream);
        }

        public void Dispose()
        {
            _internalStream?.Dispose();
            _internalStream = null;
        }
    }
}

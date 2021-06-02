using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Backups;

namespace Raven.Server.Documents.PeriodicBackup
{
    public class ProgressableStreamContent : HttpContent
    {
        private const int DefaultBufferSize = 4096;

        private readonly Stream _content;
        private readonly Progress _progress;

        public long Uploaded;

        public ProgressableStreamContent(Stream content, Progress progress)
        {
            _content = content ?? throw new ArgumentNullException("content");
            _progress = progress;
        }

        protected override Task SerializeToStreamAsync(Stream stream, TransportContext context)
        {
            return Task.Run(async() =>
            {
                var buffer = new byte[DefaultBufferSize];

                _progress?.UploadProgress.ChangeState(UploadState.PendingUpload);

                await using (_content)
                {
                    while (true)
                    {
                        var length = await _content.ReadAsync(buffer, 0, buffer.Length);
                        if (length <= 0)
                            break;

                        await stream.WriteAsync(buffer, 0, length);

                        Uploaded += length;

                        _progress?.UploadProgress.ChangeState(UploadState.Uploading);
                        _progress?.UploadProgress.UpdateUploaded(length);
                        _progress?.OnUploadProgress();
                    }
                }

                _progress?.UploadProgress.ChangeState(UploadState.PendingResponse);
            });
        }

        protected override bool TryComputeLength(out long length)
        {
            length = _content.Length;
            return true;
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
                _content.Dispose();

            base.Dispose(disposing);
        }
    }
}

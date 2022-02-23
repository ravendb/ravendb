using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Client.Extensions;

namespace Raven.Client.Util
{
    internal class StreamExposerContent : HttpContent
    {
        public readonly Task<Stream> OutputStream;
        private readonly TaskCompletionSource<Stream> _outputStreamTcs;
        protected readonly TaskCompletionSource<object> _done;

        public bool IsDone => _done.Task.IsCompleted;

        public StreamExposerContent()
        {
            _outputStreamTcs = new TaskCompletionSource<Stream>(TaskCreationOptions.RunContinuationsAsynchronously);
            OutputStream = _outputStreamTcs.Task;
            _done = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        public bool Complete() => _done.TrySetResult(null);

        protected override Task SerializeToStreamAsync(Stream stream, TransportContext context)
        {
            _outputStreamTcs.TrySetResult(stream);

            return _done.Task;
        }

        protected override bool TryComputeLength(out long length)
        {
            length = -1;
            return false;
        }

        public void ErrorOnRequestStart(Exception exception)
        {
            _outputStreamTcs.TrySetException(exception);
        }

        public void ErrorOnProcessingRequest(Exception exception)
        {
            _done.TrySetException(exception);
        }

        protected override void Dispose(bool disposing)
        {
            _done.TrySetCanceled();

            //after dispose we don't care for unobserved exceptions
            _done.Task.IgnoreUnobservedExceptions();
            _outputStreamTcs.Task.IgnoreUnobservedExceptions();
        }
    }
}

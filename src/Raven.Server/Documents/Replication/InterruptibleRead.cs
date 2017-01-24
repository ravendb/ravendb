using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server.Documents.Replication
{
    public class InterruptibleRead : IDisposable
    {
        private Task<Result> _prevCall;
        private Task[] _waitableTasks;
        private readonly DocumentsContextPool _contextPool;
        private readonly Stream _stream;

        public struct Result : IDisposable
        {
            public BlittableJsonReaderObject Document;
            public IDisposable ReturnContext;
            public DocumentsOperationContext Context;
            public bool Timeout;
            public bool Interrupted;
            public void Dispose()
            {
                Document?.Dispose();
                ReturnContext?.Dispose();
            }
        }

        public InterruptibleRead(DocumentsContextPool contextPool, Stream stream)
        {
            _contextPool = contextPool;
            _stream = stream;
        }

        public Result ParseToMemory(
            AsyncManualResetEvent interrupt,
            string debugTag, 
            int timeout, 
            JsonOperationContext.ManagedPinnedBuffer buffer, 
            CancellationToken token)
        {
            if (_prevCall == null)
            {
                _prevCall = ReadNextObject(debugTag, buffer);
            }
            if (_waitableTasks == null)
                _waitableTasks = new Task[2];

            _waitableTasks[0] = _prevCall;
            _waitableTasks[1] = interrupt.WaitAsync();

            var state = Task.WaitAny(_waitableTasks, timeout, token);

            if (state == -1)
                return new Result {Timeout = true};

            if (state != 0)
                return new Result {Interrupted = true};

            try
            {
                return _prevCall.Result;
            }
            finally
            {
                _prevCall = null;
            }
        }

        private async Task<Result> ReadNextObject(string debugTag, JsonOperationContext.ManagedPinnedBuffer buffer)
        {
            DocumentsOperationContext context;
            var retCtx = _contextPool.AllocateOperationContext(out context);
            try
            {
                var jsonReaderObject =
                    await context.ParseToMemoryAsync(_stream, debugTag, BlittableJsonDocumentBuilder.UsageMode.None, buffer);
                return new Result
                {
                    Document = jsonReaderObject,
                    ReturnContext = retCtx,
                    Context = context
                };
            }
            catch (Exception)
            {
                retCtx.Dispose();
                throw;
            }
        }

        public void Dispose()
        {
            if (_prevCall == null)
                return;

            try
            {
                using (_prevCall.Result)
                {

                }
            }
            catch (Exception)
            {
                // explicitly ignoring this
            }
            finally
            {
                _prevCall = null;
            }
        }
    }
}
using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server.Documents.Replication
{
    public struct InterruptibleRead
    {
        private readonly AsyncManualResetEvent _interrupt;
        private Task<Result> _prevCall;
        private Task[] _waitableTasks;
        private readonly DocumentsContextPool _contextPool;

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

        public InterruptibleRead(AsyncManualResetEvent interrupt, DocumentsContextPool contextPool) : this()
        {
            _interrupt = interrupt;
            _contextPool = contextPool;
        }

        public Result ParseToMemory(
            Stream stream,
            string debugTag, 
            int timeout, 
            JsonOperationContext.ManagedPinnedBuffer buffer, 
            CancellationToken token)
        {
            if (_prevCall == null)
            {
                _prevCall = ReadNextObject(stream, debugTag, buffer);
            }
            if (_waitableTasks == null)
                _waitableTasks = new Task[2];

            _waitableTasks[0] = _prevCall;
            _waitableTasks[1] = _interrupt.WaitAsync();

            var state = Task.WaitAny(_waitableTasks, timeout, token);

            if (state == -1)
                return new Result {Timeout = true};

            if (state != 0)
                return new Result {Interrupted = true};

            var result = _prevCall.Result;
            _prevCall = null;
            return result;
        }

        private async Task<Result> ReadNextObject(Stream stream, string debugTag, JsonOperationContext.ManagedPinnedBuffer buffer)
        {
            DocumentsOperationContext context;
            var retCtx = _contextPool.AllocateOperationContext(out context);
            try
            {
                var jsonReaderObject =
                    await context.ParseToMemoryAsync(stream, debugTag, BlittableJsonDocumentBuilder.UsageMode.None, buffer);
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
    }
}
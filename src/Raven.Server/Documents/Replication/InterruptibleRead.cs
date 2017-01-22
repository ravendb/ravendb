using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server.Documents.Replication
{
    public struct InterruptibleRead
    {
        private readonly AsyncManualResetEvent _interrupt;
        private readonly JsonOperationContext _context;
        private Task<BlittableJsonReaderObject> _prevCall;
        private Task[] _waitableTasks;

        public struct Result : IDisposable
        {
            public BlittableJsonReaderObject Document;
            public bool Timeout;
            public bool Interrupted;
            public void Dispose()
            {
                Document?.Dispose();
            }
        }

        public InterruptibleRead(AsyncManualResetEvent interrupt, JsonOperationContext context) : this()
        {
            _interrupt = interrupt;
            _context = context;
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
                _prevCall = _context.ParseToMemoryAsync(stream, debugTag,BlittableJsonDocumentBuilder.UsageMode.None, buffer);
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
            return new Result { Document = result };
        }

    }
}
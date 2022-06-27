using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Json;
using Sparrow.Server;
using Sparrow.Utils;

namespace Raven.Server.Documents.Replication
{
    public class InterruptibleRead<TContextPool, TOperationContext> : IDisposable
    where TContextPool : JsonContextPoolBase<TOperationContext>
    where TOperationContext : JsonOperationContext
    {
        private bool _isDisposed;

        private Task<Result> _prevCall;
        private readonly Dictionary<AsyncManualResetEvent, Task<Task>> _previousWait = new Dictionary<AsyncManualResetEvent, Task<Task>>();

        private readonly TContextPool _contextPool;
        private readonly Stream _stream;

        public struct Result : IDisposable
        {
            public BlittableJsonReaderObject Document;
            public IDisposable ReturnContext;
            public TOperationContext Context;
            public bool Timeout;
            public bool Interrupted;

            public void Dispose()
            {
                Document?.Dispose();
                Document = null;

                ReturnContext?.Dispose();
                ReturnContext = null;
            }
        }

        public InterruptibleRead(TContextPool contextPool, Stream stream)
        {
            _contextPool = contextPool;
            _stream = stream;
        }

        public Result ParseToMemory(
            AsyncManualResetEvent interrupt,
            string debugTag,
            int timeout,
            JsonOperationContext.MemoryBuffer buffer,
            CancellationToken token)
        {
            if (_prevCall == null)
            {
                _prevCall = ReadNextObject(debugTag, buffer, token);
                _previousWait.Clear();
            }

            if (_prevCall.IsCompleted)
            {
                return ReturnAndClearValue();
            }

            if (interrupt != null)
            {
                DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Shiran, DevelopmentHelper.Severity.Normal, "check if we can avoid the above 'if' (fails only for sharding)");

                if (_previousWait.TryGetValue(interrupt, out Task<Task> task) == false)
                {
                    _previousWait[interrupt] = task = Task.WhenAny(_prevCall, interrupt.WaitAsync());
                }

                if (task.Wait(timeout, token) == false)
                    return new Result { Timeout = true };

                if (task.Result != _prevCall)
                {
                    _previousWait.Remove(interrupt);
                    return new Result { Interrupted = true };
                }

            }

            return ReturnAndClearValue();
        }

        private Result ReturnAndClearValue()
        {
            try
            {
                return _prevCall.Result;
            }
            catch (ObjectDisposedException)
            {
                //we are disposing, so don't care about this exception.
                //this is thrown from inside ParseToMemoryAsync() call
                //from inside of ReadNextObject() when disposing (thrown from disposed stream basically)
                return new Result
                {
                    Interrupted = true
                };
            }
            finally
            {
                _prevCall = null;
                _previousWait.Clear();
            }
        }

        private async Task<Result> ReadNextObject(string debugTag, JsonOperationContext.MemoryBuffer buffer, CancellationToken token)
        {
            var returnContext = _contextPool.AllocateOperationContext(out TOperationContext context);
            try
            {
                var json = await context.ParseToMemoryAsync(_stream, debugTag, BlittableJsonDocumentBuilder.UsageMode.None, buffer, token: token);
                return new Result
                {
                    Document = json,
                    ReturnContext = returnContext,
                    Context = context
                };
            }
            catch (Exception)
            {
                returnContext.Dispose();
                throw;
            }
        }

        public void Dispose()
        {
            if (_isDisposed)
                return;

            _isDisposed = true;

            SafelyDispose(_stream); // need to dispose the current stream to abort the operation
            SafelyDispose(_prevCall?.Result);

            _prevCall = null;

            static void SafelyDispose(IDisposable toDispose)
            {
                try
                {
                    toDispose?.Dispose();
                }
                catch
                {
                    // explicitly ignoring this
                }
            }
        }
    }
}

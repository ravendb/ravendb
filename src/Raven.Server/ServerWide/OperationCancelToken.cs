using System;
using System.Threading;
using Raven.Server.Extensions;

namespace Raven.Server.ServerWide
{
    public class OperationCancelToken : IDisposable
    {
        private readonly CancellationTokenSource _cts;
        private readonly CancellationTokenSource _linkedCts;
        private readonly CancellationTimeout _timeout;

        public OperationCancelToken(TimeSpan cancelAfter, CancellationToken resourceShutdown)
        {
            _cts = new CancellationTokenSource();
            _timeout = _cts.TimeoutAfter(cancelAfter);

            _linkedCts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token, resourceShutdown);
        }

        public CancellationToken Cancel => _linkedCts.Token;

        public void Dispose()
        {
            _linkedCts.Dispose();
            _timeout.Dispose();
            _cts.Dispose();
        }
    }
}
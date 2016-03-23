using System;
using System.Threading;

namespace Raven.Server.ServerWide
{
    public class OperationCancelToken : IDisposable
    {
        private readonly CancellationTokenSource _cts;
        private readonly CancellationTokenSource _linkedCts;

        public OperationCancelToken(TimeSpan cancelAfter, CancellationToken resourceShutdown)
        {
            _cts = new CancellationTokenSource(cancelAfter);

            _linkedCts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token, resourceShutdown);
        }

        public CancellationToken Cancel => _linkedCts.Token;

        public void Dispose()
        {
            _linkedCts.Dispose();
            _cts.Dispose();
        }
    }
}
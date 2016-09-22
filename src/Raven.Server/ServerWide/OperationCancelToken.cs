using System;
using System.Threading;

namespace Raven.Server.ServerWide
{
    public class OperationCancelToken : IDisposable
    {
        public static OperationCancelToken None = new OperationCancelToken(CancellationToken.None);

        private readonly CancellationTokenSource _cts;
        private readonly CancellationTokenSource _linkedCts;

        public OperationCancelToken(TimeSpan cancelAfter, CancellationToken resourceShutdown)
        {
            _cts = new CancellationTokenSource(cancelAfter);
            _linkedCts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token, resourceShutdown);

            Token = _linkedCts.Token;
        }

        public OperationCancelToken(CancellationToken token)
        {
            _cts = new CancellationTokenSource();
            _linkedCts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token, token);
            Token = _linkedCts.Token;
        }

        public readonly CancellationToken Token;

        public void Cancel()
        {
            _linkedCts.Cancel();
        }

        public void Dispose()
        {
            _linkedCts?.Dispose();
            _cts?.Dispose();
        }
    }
}
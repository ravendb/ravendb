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

            Cancel = _linkedCts.Token;
        }

        private OperationCancelToken(CancellationToken token)
        {
            Cancel = token;
        }

        public readonly CancellationToken Cancel;

        public void Dispose()
        {
            _linkedCts?.Dispose();
            _cts?.Dispose();
        }
    }
}
using System;
using System.Threading.Tasks;
using Raven.Abstractions.Util;
using Raven.Client.Connection.Async;
using Raven.Client.Data;
using Raven.Client.Exceptions;

namespace Raven.Client.Connection
{
    public class Operation : IObserver<OperationStatusChangeNotification>
    {
        private readonly AsyncServerClient _asyncServerClient;
        private readonly long _id;
        private IDisposable _subscription;
        private readonly TaskCompletionSource<IOperationResult> _result = new TaskCompletionSource<IOperationResult>();

        public Operation(long id)
        {
            _id = id;
            throw new NotImplementedException();
        }

        public Operation(AsyncServerClient asyncServerClient, long id)
        {
            _asyncServerClient = asyncServerClient;
            _id = id;
        }

        public async Task Initialize()
        {
            await _asyncServerClient.changes.Value.ConnectionTask.ConfigureAwait(false);
            var observableWithTask = _asyncServerClient.changes.Value.ForOperationId(_id);
            _subscription = observableWithTask.Subscribe(this);
        }

        internal long Id => _id;

        public Action<IOperationProgress> OnProgressChanged;

        public void OnNext(OperationStatusChangeNotification notification)
        {
            var onProgress = OnProgressChanged;

            switch (notification.State.Status)
            {
                case OperationStatus.InProgress:
                    if (onProgress != null && notification.State.Progress != null)
                    {
                        onProgress(notification.State.Progress);
                    }
                    break;
                case OperationStatus.Completed:
                    _subscription.Dispose();
                    _result.SetResult(notification.State.Result);
                    break;
                case OperationStatus.Faulted:
                    _subscription.Dispose();
                    var exceptionResult = notification.State.Result as OperationExceptionResult;
                    if(exceptionResult?.StatusCode == 409)
                        _result.SetException(new DocumentInConflictException(exceptionResult.Message));
                    else
                        _result.SetException(new InvalidOperationException(exceptionResult?.Message));
                    break;
                case OperationStatus.Canceled:
                    _subscription.Dispose();
                    _result.SetException(new OperationCanceledException());
                    break;
            }
        }

        public void OnError(Exception error)
        {
        }

        public void OnCompleted()
        {
        }

        public virtual Task<IOperationResult> WaitForCompletionAsync()
        {
            return _result.Task;
        }

        public virtual IOperationResult WaitForCompletion()
        {
            return AsyncHelpers.RunSync(WaitForCompletionAsync);
        }
    }
}

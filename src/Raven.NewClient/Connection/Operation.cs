using System;
using System.Threading.Tasks;
using Raven.NewClient.Abstractions.Extensions;
using Raven.NewClient.Abstractions.Util;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Client.Exceptions;
using Raven.NewClient.Client.Helpers;
using Raven.NewClient.Client.Http;
using Raven.NewClient.Commands;
using Sparrow.Json;

namespace Raven.NewClient.Connection
{
    public class Operation : IObserver<OperationStatusChangeNotification>
    {
        private readonly RequestExecuter _requestExecuter;
        private readonly DocumentConvention _conventions;
        private readonly long _id;
        private IDisposable _subscription;
        private readonly TaskCompletionSource<IOperationResult> _result = new TaskCompletionSource<IOperationResult>();

        public Action<IOperationProgress> OnProgressChanged;
        private bool _work;
        private JsonOperationContext _context;

        internal long Id => _id;

        public Operation(RequestExecuter requestExecuter, DocumentConvention conventions, long id)
        {
            DevelopmentHelper.TimeBomb(); // use changes API

            _requestExecuter = requestExecuter;
            _conventions = conventions;
            _id = id;
            _work = true;
        }

        private async Task Initialize()
        {
            try
            {
                //await _asyncServerClient.changes.Value.ConnectionTask.ConfigureAwait(false);
                //var observableWithTask = _asyncServerClient.changes.Value.ForOperationId(_id);
                //_subscription = observableWithTask.Subscribe(this);
                //await FetchOperationStatus().ConfigureAwait(false);

                _subscription = new DisposableAction(() => _work = false);

                while (_work)
                {
                    await FetchOperationStatus().ConfigureAwait(false);
                    if (_work == false)
                        break;

                    await Task.Delay(500).ConfigureAwait(false);
                }
            }
            catch (Exception e)
            {
                _result.TrySetException(e);
            }
        }

        /// <summary>
        /// Since operation might complete before we subscribe to it, 
        /// fetch operation status but only once  to avoid race condition
        /// If we receive notification using changes API meanwhile, ignore fetched status
        /// to avoid issues with non monotonic increasing progress
        /// </summary>
        private async Task FetchOperationStatus()
        {
            var command = new GetOperationStateCommand(_conventions, _id);

            await _requestExecuter.ExecuteAsync(command, _context);

            OnNext(new OperationStatusChangeNotification
            {
                OperationId = _id,
                State = command.Result
            });
        }

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
                    _result.TrySetResult(notification.State.Result);
                    break;
                case OperationStatus.Faulted:
                    _subscription.Dispose();
                    var exceptionResult = notification.State.Result as OperationExceptionResult;
                    if (exceptionResult?.StatusCode == 409)
                        _result.TrySetException(new DocumentInConflictException(exceptionResult.Message));
                    else
                        _result.TrySetException(new InvalidOperationException(exceptionResult?.Message));
                    break;
                case OperationStatus.Canceled:
                    _subscription.Dispose();
                    _result.TrySetCanceled();
                    break;
            }
        }

        public void OnError(Exception error)
        {
        }

        public void OnCompleted()
        {
        }

        public async Task<IOperationResult> WaitForCompletionAsync(TimeSpan? timeout = null)
        {
            using (_requestExecuter.ContextPool.AllocateOperationContext(out _context))
            {
#pragma warning disable 4014
                Task.Factory.StartNew(Initialize);
#pragma warning restore 4014

                var completed = await _result.Task.WaitWithTimeout(timeout).ConfigureAwait(false);
                if (completed == false)
                    throw new TimeoutException($"After {timeout}, did not get a reply for operation " + _id);

                return await _result.Task.ConfigureAwait(false);
            }
        }

        public IOperationResult WaitForCompletion(TimeSpan? timeout = null)
        {
            return AsyncHelpers.RunSync(() => WaitForCompletionAsync(timeout));
        }
    }
}

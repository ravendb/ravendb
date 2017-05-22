using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Exceptions;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations
{
    public class Operation : IObserver<OperationStatusChange>
    {
        private readonly RequestExecutor _requestExecutor;
        private readonly Func<IDatabaseChanges> _changes;
        private readonly DocumentConventions _conventions;
        private readonly long _id;
        private readonly TaskCompletionSource<IOperationResult> _result = new TaskCompletionSource<IOperationResult>(TaskCreationOptions.RunContinuationsAsynchronously);

        public Action<IOperationProgress> OnProgressChanged;
        private JsonOperationContext _context;
        private IDisposable _subscription;

        internal long Id => _id;

        public Operation(RequestExecutor requestExecutor, Func<IDatabaseChanges> changes, DocumentConventions conventions, long id)
        {
            _requestExecutor = requestExecutor;
            _changes = changes;
            _conventions = conventions;
            _id = id;
        }

        private async Task Initialize()
        {
            try
            {
                var changes = await _changes().EnsureConnectedNow().ConfigureAwait(false);
                var observable = changes.ForOperationId(_id);
                _subscription = observable.Subscribe(this);
                await observable.EnsureSubscribedNow().ConfigureAwait(false);

                await FetchOperationStatus().ConfigureAwait(false);
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

            await _requestExecutor.ExecuteAsync(command, _context);

            OnNext(new OperationStatusChange
            {
                OperationId = _id,
                State = command.Result
            });
        }

        public void OnNext(OperationStatusChange change)
        {
            var onProgress = OnProgressChanged;

            switch (change.State.Status)
            {
                case OperationStatus.InProgress:
                    if (onProgress != null && change.State.Progress != null)
                    {
                        onProgress(change.State.Progress);
                    }
                    break;
                case OperationStatus.Completed:
                    _subscription?.Dispose();
                    _result.TrySetResult(change.State.Result);
                    break;
                case OperationStatus.Faulted:
                    _subscription?.Dispose();
                    var exceptionResult = (OperationExceptionResult)change.State.Result;
                    Debug.Assert(exceptionResult != null);
                    _result.TrySetException(ExceptionDispatcher.Get(exceptionResult.Message, exceptionResult.Error, exceptionResult.Type, exceptionResult.StatusCode));
                    break;
                case OperationStatus.Canceled:
                    _subscription?.Dispose();
                    _result.TrySetCanceled();
                    break;
            }
        }

        public void OnError(Exception error)
        {
        }

        public void OnCompleted()
        {
            _result.Task.IgnoreUnobservedExceptions();
        }

        public Task<IOperationResult> WaitForCompletionAsync(TimeSpan? timeout = null)
        {
            return WaitForCompletionAsync<IOperationResult>(timeout);
        }

        public async Task<TResult> WaitForCompletionAsync<TResult>(TimeSpan? timeout = null)
            where TResult : IOperationResult
        {
            using (_requestExecutor.ContextPool.AllocateOperationContext(out _context))
            {
#pragma warning disable 4014
                Task.Factory.StartNew(Initialize);
#pragma warning restore 4014

                var completed = await _result.Task.WaitWithTimeout(timeout).ConfigureAwait(false);
                if (completed == false)
                    throw new TimeoutException($"After {timeout}, did not get a reply for operation " + _id);

                return (TResult)await _result.Task.ConfigureAwait(false);
            }
        }

        public IOperationResult WaitForCompletion(TimeSpan? timeout = null)
        {
            return WaitForCompletion<IOperationResult>(timeout);
        }

        public TResult WaitForCompletion<TResult>(TimeSpan? timeout = null)
            where TResult : IOperationResult
        {
            return AsyncHelpers.RunSync(() => WaitForCompletionAsync<TResult>(timeout));
        }
    }
}

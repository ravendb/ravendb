using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Conventions;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Changes;
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
        private readonly SemaphoreSlim _lock = new SemaphoreSlim(1, 1);

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
                await Process().ConfigureAwait(false);
            }
            catch (Exception e)
            {
                _result.TrySetException(e);
            }
        }

        protected virtual async Task Process()
        {
            var changes = await _changes().EnsureConnectedNow().ConfigureAwait(false);
            var observable = changes.ForOperationId(_id);
            _subscription = observable.Subscribe(this);
            await observable.EnsureSubscribedNow().ConfigureAwait(false);

            await FetchOperationStatus().ConfigureAwait(false);
        }

        protected virtual void StopProcessing()
        {
            _subscription?.Dispose();
            _subscription = null;
        }

        /// <summary>
        /// Since operation might complete before we subscribe to it, 
        /// fetch operation status but only once  to avoid race condition
        /// If we receive notification using changes API meanwhile, ignore fetched status
        /// to avoid issues with non monotonic increasing progress
        /// </summary>
        protected async Task FetchOperationStatus()
        {
            await _lock.WaitAsync().ConfigureAwait(false);

            OperationState state;
            try
            {
                if (_result.Task.IsCompleted)
                    return;

                var command = GetOperationStateCommand(_conventions, _id);

                await _requestExecutor.ExecuteAsync(command, _context).ConfigureAwait(false);

                state = command.Result;
            }
            finally
            {
                _lock.Release();
            }

            OnNext(new OperationStatusChange
            {
                OperationId = _id,
                State = state
            });
        }

        protected virtual RavenCommand<OperationState> GetOperationStateCommand(DocumentConventions conventions, long id)
        {
            return new GetOperationStateOperation.GetOperationStateCommand(conventions, id);
        }

        public void OnNext(OperationStatusChange change)
        {
            _lock.Wait();

            try
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
                        StopProcessing();
                        _result.TrySetResult(change.State.Result);
                        break;
                    case OperationStatus.Faulted:
                        StopProcessing();
                        var exceptionResult = (OperationExceptionResult)change.State.Result;
                        Debug.Assert(exceptionResult != null);
                        _result.TrySetException(ExceptionDispatcher.Get(exceptionResult.Message, exceptionResult.Error, exceptionResult.Type,
                            exceptionResult.StatusCode));
                        break;
                    case OperationStatus.Canceled:
                        StopProcessing();
                        _result.TrySetCanceled();
                        break;
                }
            }
            finally
            {
                _lock.Release();
            }
        }

        public void OnError(Exception error)
        {
            if (error is ChangeProcessingException)
                return;

            StopProcessing();

#pragma warning disable 4014
            Task.Factory.StartNew(Initialize);
#pragma warning restore 4014
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
                {
                    await _lock.WaitAsync().ConfigureAwait(false);

                    try
                    {
                        StopProcessing();
                    }
                    catch
                    {
                        // ignoring
                    }
                    finally
                    {
                        _lock.Release();
                    }
                    
                    throw new TimeoutException($"After {timeout}, did not get a reply for operation " + _id);
                }

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

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
using Sparrow.Utils;

namespace Raven.Client.Documents.Operations
{
    public class Operation : IObserver<OperationStatusChange>
    {
        private readonly RequestExecutor _requestExecutor;
        private readonly Func<IDatabaseChanges> _changes;
        private readonly DocumentConventions _conventions;
        private readonly Task _additionalTask;
        private readonly long _id;
        private TaskCompletionSource<IOperationResult> _result = new TaskCompletionSource<IOperationResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly SemaphoreSlim _lock = new SemaphoreSlim(1, 1);

        public Action<IOperationProgress> OnProgressChanged;
        private JsonOperationContext _context;
        private IDisposable _subscription;

        internal long Id => _id;
        internal string NodeTag;

        public OperationStatusFetchMode StatusFetchMode { get; protected set; }

        private bool _isProcessing;

        public Operation(RequestExecutor requestExecutor, Func<IDatabaseChanges> changes, DocumentConventions conventions, long id, string nodeTag = null)
            : this(requestExecutor, changes, conventions, id, nodeTag: nodeTag, additionalTask: null)
        {
        }

        internal Operation(RequestExecutor requestExecutor, Func<IDatabaseChanges> changes, DocumentConventions conventions, long id, string nodeTag, Task additionalTask)
        {
            _requestExecutor = requestExecutor;
            _changes = changes;
            _conventions = conventions;
            _additionalTask = additionalTask ?? Task.CompletedTask;
            _id = id;
            NodeTag = nodeTag;

            StatusFetchMode = _conventions.OperationStatusFetchMode;
            _isProcessing = true;
        }

        private async Task Initialize()
        {
            try
            {
                await Process().ConfigureAwait(false);
            }
            catch (Exception e)
            {
                await StopProcessingUnderLock(e).ConfigureAwait(false);
            }
        }

        private async Task<Task<IOperationResult>> InitializeResult()
        {
            await _lock.WaitAsync().ConfigureAwait(false);
            try
            {
                if (_result.Task.IsCompleted)
                    _result = new TaskCompletionSource<IOperationResult>(TaskCreationOptions.RunContinuationsAsynchronously);

                return _result.Task;
            }
            finally
            {
                _lock.Release();
            }
        }

        protected virtual async Task Process()
        {
            _isProcessing = true;

            switch (StatusFetchMode)
            {
                case OperationStatusFetchMode.ChangesApi:
                    var changes = await _changes().EnsureConnectedNow().ConfigureAwait(false);
                    var observable = changes.ForOperationId(_id);
                    _subscription = observable.Subscribe(this);
                    await observable.EnsureSubscribedNow().ConfigureAwait(false);
                    changes.ConnectionStatusChanged += OnConnectionStatusChanged;

                    // We start the operation before we subscribe,
                    // so if we subscribe after the operation was already completed we will miss the notification for it. 
                    await FetchOperationStatus().ConfigureAwait(false);

                    break;
                case OperationStatusFetchMode.Polling:
                    while (_isProcessing)
                    {
                        await FetchOperationStatus().ConfigureAwait(false);
                        if (_isProcessing == false)
                            break;

                        await TimeoutManager.WaitFor(TimeSpan.FromSeconds(1)).ConfigureAwait(false);
                    }
                    break;
                default:
                    throw new NotSupportedException($"Invalid operation fetch status mode: '{StatusFetchMode}'");
            }
        }

        private void OnConnectionStatusChanged(object sender, EventArgs e)
        {
            AsyncHelpers.RunSync(OnConnectionStatusChangedAsync);
        }

        private async Task OnConnectionStatusChangedAsync()
        {
            try
            {
                await FetchOperationStatus().ConfigureAwait(false);
            }
            catch (Exception e)
            {
                await StopProcessingUnderLock(e).ConfigureAwait(false);
            }
        }

        private async Task StopProcessingUnderLock(Exception e = null)
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
                if (e != null)
                    _result.TrySetException(e);
                _lock.Release();
            }
        }

        protected virtual void StopProcessing()
        {
            if (StatusFetchMode == OperationStatusFetchMode.ChangesApi)
            {
                _changes().ConnectionStatusChanged -= OnConnectionStatusChanged;
                _subscription?.Dispose();
                _subscription = null;
            }

            _isProcessing = false;
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

            OperationState state = null;
            try
            {
                for (var i = 0; i < 10; i++)
                {
                    if (_result.Task.IsCompleted)
                        return;

                    var command = GetOperationStateCommand(_conventions, _id, NodeTag);

                    await _requestExecutor.ExecuteAsync(command, _context, sessionInfo: null, token: CancellationToken.None).ConfigureAwait(false);

                    state = command.Result;

                    // in most of the cases this will only perform one loop
                    // but for operations like smuggler-ones there is a race probability
                    // between sending the request to perform the operation
                    // and registering it
                    // this is why we are trying to get the state few times
                    if (state != null)
                        break;

                    await Task.Delay(500).ConfigureAwait(false);
                }
            }
            finally
            {
                _lock.Release();
            }

            if (state == null)
                throw new InvalidOperationException($"Could not fetch state of operation '{_id}' from node '{NodeTag}'.");

            OnNext(new OperationStatusChange
            {
                OperationId = _id,
                State = state
            });
        }

        protected virtual RavenCommand<OperationState> GetOperationStateCommand(DocumentConventions conventions, long id, string nodeTag = null)
        {
            return new GetOperationStateOperation.GetOperationStateCommand(conventions, id, nodeTag);
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
                        if (_additionalTask.IsFaulted)
                        {
                            _result.TrySetException(_additionalTask.Exception);
                            break;
                        }

                        var exceptionResult = (OperationExceptionResult)change.State.Result;
                        Debug.Assert(exceptionResult != null);
                        var ex = new ExceptionDispatcher.ExceptionSchema
                        {
                            Error = exceptionResult.Error,
                            Message = exceptionResult.Message,
                            Type = exceptionResult.Type,
                            Url = _requestExecutor.Url
                        };
                        _result.TrySetException(ExceptionDispatcher.Get(ex, exceptionResult.StatusCode));
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
                var result = await InitializeResult().ConfigureAwait(false);

#pragma warning disable 4014
                Task.Factory.StartNew(Initialize);
#pragma warning restore 4014
                bool completed;
                try
                {
                    completed = await result.WaitWithTimeout(timeout).ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    await StopProcessingUnderLock(e).ConfigureAwait(false);
                    completed = true;
                }

                if (completed == false)
                {
                    await StopProcessingUnderLock().ConfigureAwait(false);
                    throw new TimeoutException($"After {timeout}, did not get a reply for operation " + _id);
                }

                await _additionalTask.ConfigureAwait(false);
                return (TResult)await result.ConfigureAwait(false); // already done waiting but in failure we want the exception itself and not AggregateException 
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

using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Commands;
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
    public class Operation<TResult> : Operation
    {
        internal Operation(RequestExecutor requestExecutor, Func<IDatabaseChanges> changes, DocumentConventions conventions, TResult result, long id, string nodeTag = null)
            : base(requestExecutor, changes, conventions, id, nodeTag)
        {
            Result = result;
        }

        internal Operation(RequestExecutor requestExecutor, Func<IDatabaseChanges> changes, DocumentConventions conventions, TResult result, long id, string nodeTag, Task afterOperationCompleted)
            : base(requestExecutor, changes, conventions, id, nodeTag, afterOperationCompleted)
        {
            Result = result;
        }

        public TResult Result { get; }
    }

    public class Operation : IObserver<OperationStatusChange>
    {
        private readonly RequestExecutor _requestExecutor;
        private readonly TimeSpan? _requestExecutorDefaultTimeout;

        private readonly Func<IDatabaseChanges> _changes;
        private readonly DocumentConventions _conventions;
        private readonly Task _afterOperationCompleted;
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
            : this(requestExecutor, changes, conventions, id, nodeTag: nodeTag, afterOperationCompleted: null)
        {
        }

        internal Operation(RequestExecutor requestExecutor, Func<IDatabaseChanges> changes, DocumentConventions conventions, long id, string nodeTag, Task afterOperationCompleted)
        {
            _requestExecutor = requestExecutor;
            _requestExecutorDefaultTimeout = requestExecutor.DefaultTimeout;
            _changes = changes;
            _conventions = conventions;
            _afterOperationCompleted = afterOperationCompleted ?? Task.CompletedTask;
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
                    _changes().ConnectionStatusChanged += OnConnectionStatusChanged;

                    if (_requestExecutor.ForTestingPurposes?.BeforeFetchOperationStatus != null)
                        await _requestExecutor.ForTestingPurposes.BeforeFetchOperationStatus.ConfigureAwait(false);

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
            if (e is DatabaseChanges.OnReconnect)
                AsyncHelpers.RunSync(OnConnectionStatusChangedAsync);
        }

        private async Task OnConnectionStatusChangedAsync()
        {
            try
            {
                await FetchOperationStatus(shouldThrowOnNoStatus: false).ConfigureAwait(false);
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
        protected async Task FetchOperationStatus(bool shouldThrowOnNoStatus = true)
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
            {
                if (shouldThrowOnNoStatus)
                    throw new InvalidOperationException($"Could not fetch state of operation '{_id}' from node '{NodeTag}'.");

                return;
            }

            OnNext(new OperationStatusChange
            {
                OperationId = _id,
                State = state
            });
        }

        protected virtual RavenCommand<OperationState> GetOperationStateCommand(DocumentConventions conventions, long id, string nodeTag = null)
        {
            return new GetOperationStateOperation.GetOperationStateCommand(id, nodeTag);
        }

        protected virtual RavenCommand GetKillOperationCommand(long id, string nodeTag = null)
        {
            return new KillOperationCommand(id, nodeTag);
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
                        if (_afterOperationCompleted.IsFaulted)
                        {
                            // we want the exception itself and not AggregateException
                            _result.TrySetException(_afterOperationCompleted.Exception.ExtractSingleInnerException());
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
            if (timeout == null)
                return await WaitForCompletionAsync<TResult>(CancellationToken.None).ConfigureAwait(false);

            using (var cts = new CancellationTokenSource(timeout.Value))
                return await WaitForCompletionAsync<TResult>(cts.Token).ConfigureAwait(false);
        }

        public Task<IOperationResult> WaitForCompletionAsync(CancellationToken token)
        {
            return WaitForCompletionAsync<IOperationResult>(token);
        }

        public async Task<TResult> WaitForCompletionAsync<TResult>(CancellationToken token)
            where TResult : IOperationResult
        {
            using (_requestExecutor.ContextPool.AllocateOperationContext(out _context))
            {
                var result = await InitializeResult().ConfigureAwait(false);

                var initTask = Task.Run(Initialize);

                try
                {
                    try
                    {
#if NET6_0_OR_GREATER
                        await result.WaitAsync(token).ConfigureAwait(false);
                        await _afterOperationCompleted.WaitAsync(token).ConfigureAwait(false);
#else
                        await result.WithCancellation(token).ConfigureAwait(false);
                        await _afterOperationCompleted.WithCancellation(token).ConfigureAwait(false);
#endif
                    }
                    catch (TaskCanceledException e)
                    {
                        await StopProcessingUnderLock().ConfigureAwait(false);

                        var msg = token.IsCancellationRequested
                            ? $"Did not get a reply for operation '{_id}'."
                            : $"Operation '{_id}' was canceled.";

                        msg = AddTimeoutReasonMessageIfNecessary(msg);

                        if (token.IsCancellationRequested)
                            throw new TimeoutException(msg, e);

                        throw new TaskCanceledException(msg, e);
                    }
                    catch (Exception ex)
                    {
                        await StopProcessingUnderLock(ex).ConfigureAwait(false);
                    }

                    return (TResult)await result.ConfigureAwait(false); // already done waiting but in failure we want the exception itself and not AggregateException 
                }
                finally
                {
                    try
                    {
                        await initTask.ConfigureAwait(false);
                    }
                    catch
                    {
                        // ignored
                    }
                }
            }

            string AddTimeoutReasonMessageIfNecessary(string message)
            {
                if (_requestExecutorDefaultTimeout != null && _requestExecutorDefaultTimeout != RequestExecutor.GlobalHttpClientTimeout)
                    return $"{message} We noticed that request executor is set to timeout by default after '{_requestExecutorDefaultTimeout}' which might be a reason of the failure. This setting might be controlled via {nameof(DocumentConventions)}.{nameof(DocumentConventions.RequestTimeout)} convention, {nameof(DocumentStore)}.{nameof(DocumentStore.SetRequestTimeout)} method or directly via {nameof(RequestExecutor)}.{nameof(RequestExecutor.DefaultTimeout)} property.";

                return message;
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

        public IOperationResult WaitForCompletion(CancellationToken token)
        {
            return WaitForCompletion<IOperationResult>(token);
        }

        public TResult WaitForCompletion<TResult>(CancellationToken token)
            where TResult : IOperationResult
        {
            return AsyncHelpers.RunSync(() => WaitForCompletionAsync<TResult>(token));
        }

        public void Kill()
        {
            AsyncHelpers.RunSync(() => KillAsync());
        }

        public async Task KillAsync(CancellationToken token = default)
        {
            var command = GetKillOperationCommand(_id, NodeTag);

            using (_requestExecutor.ContextPool.AllocateOperationContext(out var context))
                await _requestExecutor.ExecuteAsync(command, context, token: token).ConfigureAwait(false);
        }
    }
}

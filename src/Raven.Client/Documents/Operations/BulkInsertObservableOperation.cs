using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Changes;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations;

public class BulkInsertObservableOperation : IObserver<OperationStatusChange>, IDisposable
{
    private readonly RequestExecutor _requestExecutor;
    private readonly Func<IDatabaseChanges> _changes;
    private readonly DocumentConventions _conventions;
    private readonly Task _additionalTask;
    private readonly long _id;
    private TaskCompletionSource<IOperationResult> _result = new TaskCompletionSource<IOperationResult>(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly SemaphoreSlim _lock = new SemaphoreSlim(1, 1);
    public Action<BulkInsertOnProgressEventArgs> OnProgressChanged;
    private JsonOperationContext _context;
    private IDisposable _subscription;
    internal string NodeTag;
    private bool _isProcessing;

    public BulkInsertObservableOperation(RequestExecutor requestExecutor, Func<IDatabaseChanges> changes, DocumentConventions conventions, long id, string nodeTag = null)
        : this(requestExecutor, changes, conventions, id, nodeTag: nodeTag, additionalTask: null)
    {
    }

    internal BulkInsertObservableOperation(RequestExecutor requestExecutor, Func<IDatabaseChanges> changes, DocumentConventions conventions, long id, string nodeTag,
        Task additionalTask)
    {
        _requestExecutor = requestExecutor;
        _changes = changes;
        _conventions = conventions;
        _additionalTask = additionalTask ?? Task.CompletedTask;
        _id = id;
        NodeTag = nodeTag;

        _isProcessing = true;
    }

    internal async Task Initialize()
    {
        try
        {
            await Process().ConfigureAwait(false);
        }
        catch (Exception e)
        {
            _isProcessing = false;
        }
    }
    
    protected virtual async Task Process()
    {
        _isProcessing = true;
        var changes = await _changes().EnsureConnectedNow().ConfigureAwait(false);
        var observable = changes.ForOperationId(_id);
        _subscription = observable.Subscribe(this);
        await observable.EnsureSubscribedNow().ConfigureAwait(false);
        changes.ConnectionStatusChanged += OnConnectionStatusChanged;
        
        await FetchOperationStatus().ConfigureAwait(false);
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
            _isProcessing = false;
        }
    }
    
    protected async Task FetchOperationStatus()
    {
        await _lock.WaitAsync().ConfigureAwait(false);

        OperationState state = null;
        try
        {
            for (var i = 0; i < 3; i++)
            {
                if (_result.Task.IsCompleted)
                    return;

                var command = GetOperationStateCommand(_conventions, _id, NodeTag);
                await _requestExecutor.ExecuteAsync(command, _context, sessionInfo: null, token: CancellationToken.None).ConfigureAwait(false);
                state = command.Result;
                if (state != null)
                    break;

                await Task.Delay(500).ConfigureAwait(false);
            }
        }
        finally
        {
            _lock.Release();
        }
        
        
        OnNext(new OperationStatusChange {OperationId = _id, State = state});
    }

    protected virtual RavenCommand<OperationState> GetOperationStateCommand(DocumentConventions conventions, long id, string nodeTag = null)
    {
        return new GetOperationStateOperation.GetOperationStateCommand(id, nodeTag);
    }

    public void OnNext(OperationStatusChange change)
    {
        _lock.Wait();

        try
        {
            var onProgress = OnProgressChanged;

            switch (change?.State?.Status)
            {
                case OperationStatus.InProgress:
                    if (onProgress != null && change.State.Progress != null)
                    {
                        onProgress(new BulkInsertOnProgressEventArgs(change.State.Progress as BulkInsertProgress));
                    }

                    break;
                case OperationStatus.Completed:
                    onProgress(new BulkInsertOnProgressEventArgs(change.State.Progress as BulkInsertProgress, true));
                    _result.TrySetResult(change.State.Result);
                    break;
                case OperationStatus.Faulted:
                    if (_additionalTask.IsFaulted)
                    {
                        _result.TrySetException(_additionalTask.Exception);
                        break;
                    }

                    var exceptionResult = (OperationExceptionResult)change.State.Result;
                    Debug.Assert(exceptionResult != null);
                    var ex = new ExceptionDispatcher.ExceptionSchema
                    {
                        Error = exceptionResult.Error, Message = exceptionResult.Message, Type = exceptionResult.Type, Url = _requestExecutor.Url
                    };
                    _result.TrySetException(ExceptionDispatcher.Get(ex, exceptionResult.StatusCode));
                    break;
                case OperationStatus.Canceled:
                    _result.TrySetCanceled();
                    break;
                case null:
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
    }

    public void OnCompleted()
    {
        _result.Task.IgnoreUnobservedExceptions();
    }
    
    public void Dispose()
    {
        _lock?.Dispose();
        _changes().ConnectionStatusChanged -= OnConnectionStatusChanged;
        _subscription?.Dispose();
        _subscription = null;
        _isProcessing = false;
    }
}

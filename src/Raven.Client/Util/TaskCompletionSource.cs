using System.Collections.Generic;

// ReSharper disable once CheckNamespace
namespace System.Threading.Tasks;

#if NETSTANDARD || NETSTANDARD2_0

/// <summary>
/// A simple polyfill of https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.taskcompletionsource introduced in .NET Core.
/// </summary>
internal sealed class TaskCompletionSource
{
    private readonly TaskCompletionSource<object> _tcs;

    public TaskCompletionSource(TaskCreationOptions creationOptions)
    {
#pragma warning disable RDB0008
        _tcs = new TaskCompletionSource<object>(creationOptions);
#pragma warning restore RDB0008
    }

    public Task Task => _tcs.Task;

    public void SetException(Exception exception) => _tcs.SetException(exception);

    public void SetException(IEnumerable<Exception> exceptions) => _tcs.SetException(exceptions);

    public bool TrySetException(Exception exception) => _tcs.TrySetException(exception);

    public bool TrySetException(IEnumerable<Exception> exceptions) => _tcs.TrySetException(exceptions);

    public void SetResult() => _tcs.SetResult(null);

    public bool TrySetResult() => _tcs.TrySetResult(null);

    public void SetCanceled() => _tcs.SetCanceled();

    public bool TrySetCanceled() => _tcs.TrySetCanceled();


    public bool TrySetCanceled(CancellationToken cancellationToken) => _tcs.TrySetCanceled(cancellationToken);
}
#endif

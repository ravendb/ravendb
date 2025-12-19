using System;
using System.Threading.Tasks;
using Raven.Client.Documents.Changes;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Changes;

internal sealed class ShardedDatabaseConnectionState : AbstractDatabaseConnectionState, IChangesConnectionState<BlittableJsonReaderObject>
{
    private event Action<BlittableJsonReaderObject> _onChangeNotification;

    public ShardedDatabaseConnectionState(Func<Task> onConnect, Func<Task> onDisconnect) : base(onConnect, onDisconnect)
    {
    }

    public void Send(BlittableJsonReaderObject change)
    {
        _onChangeNotification?.Invoke(change);
    }

    void IChangesConnectionState<BlittableJsonReaderObject>.RegisterEvents(Action<BlittableJsonReaderObject> onChangeNotification, Action<Exception> onError)
    {
        RegisterEventsInternal(ref _onChangeNotification, onChangeNotification, onError);
    }

    void IChangesConnectionState<BlittableJsonReaderObject>.UnregisterEvents(Action<BlittableJsonReaderObject> onChangeNotification, Action<Exception> onError)
    {
        UnregisterEventsInternal(ref _onChangeNotification, onChangeNotification, onError);
    }

    public override void Dispose()
    {
        lock (_eventLock)
        {
            base.Dispose();

            _onChangeNotification = null;
        }
    }
}

using System;
using System.Threading.Tasks;
using Raven.Client.Documents.Changes;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Changes;

internal class ShardedDatabaseConnectionState : AbstractDatabaseConnectionState, IChangesConnectionState<BlittableJsonReaderObject>
{
    private event Action<BlittableJsonReaderObject> _onChangeNotification;

    public ShardedDatabaseConnectionState(Func<Task> onConnect, Func<Task> onDisconnect) : base(onConnect, onDisconnect)
    {
    }

    public void Send(BlittableJsonReaderObject change)
    {
        _onChangeNotification?.Invoke(change);
    }

    event Action<BlittableJsonReaderObject> IChangesConnectionState<BlittableJsonReaderObject>.OnChangeNotification
    {
        add => _onChangeNotification += value;
        remove => _onChangeNotification -= value;
    }
}

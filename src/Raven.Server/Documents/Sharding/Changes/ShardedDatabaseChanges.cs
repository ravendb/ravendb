using System;
using System.Threading.Tasks;
using Raven.Client.Documents.Changes;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Changes;

internal class ShardedDatabaseChanges : AbstractDatabaseChanges<ShardedDatabaseConnectionState>
{
    public ShardedDatabaseChanges(RequestExecutor requestExecutor, string databaseName, Action onDispose, string nodeTag) 
        : base(requestExecutor, databaseName, onDispose, nodeTag)
    {
    }

    public IChangesObservable<BlittableJsonReaderObject> ForDocument(string docId)
    {
        if (string.IsNullOrWhiteSpace(docId))
            throw new ArgumentException("Value cannot be null or whitespace.", nameof(docId));

        var counter = GetOrAddConnectionState("docs/" + docId, "watch-doc", "unwatch-doc", docId);

        var taskedObservable = new ChangesObservable<BlittableJsonReaderObject, ShardedDatabaseConnectionState>(
            counter,
            filter: null);

        return taskedObservable;
    }

    public IChangesObservable<BlittableJsonReaderObject> ForAllDocuments()
    {
        var counter = GetOrAddConnectionState("all-docs", "watch-docs", "unwatch-docs", null);

        var taskedObservable = new ChangesObservable<BlittableJsonReaderObject, ShardedDatabaseConnectionState>(
            counter,
            filter: null);

        return taskedObservable;
    }

    protected override ShardedDatabaseConnectionState CreateDatabaseConnectionState(Func<Task> onConnect, Func<Task> onDisconnect) => new(onConnect, onDisconnect);

    protected override void ProcessNotification(string type, BlittableJsonReaderObject change)
    {
        foreach (var state in States.ForceEnumerateInThreadSafeManner())
        {
            state.Value.Send(change);
        }
    }
}

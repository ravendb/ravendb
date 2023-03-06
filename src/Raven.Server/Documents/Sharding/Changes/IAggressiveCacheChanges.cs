using Raven.Client.Documents.Changes;

namespace Raven.Server.Documents.Sharding.Changes;

public interface IAggressiveCacheChanges<out TChange>
{
    IChangesObservable<TChange> ForAggressiveCaching();
}

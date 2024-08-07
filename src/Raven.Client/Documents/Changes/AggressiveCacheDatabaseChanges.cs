using System;
using System.Threading.Tasks;
using Raven.Client.Http;

namespace Raven.Client.Documents.Changes;

internal class AggressiveCacheDatabaseChanges : DatabaseChanges
{
    internal AggressiveCacheDatabaseChanges(RequestExecutor requestExecutor, string databaseName, Action onDispose) : base(requestExecutor, databaseName, onDispose: onDispose, nodeTag: null)
    {
    }

    internal override void NotifyAboutReconnection(Exception e)
    {
        NotifyAboutError(e);
    }

    public async Task<AggressiveCacheDatabaseChanges> EnsureConnectedNow()
    {
        var changes = await EnsureConnectedNowAsync().ConfigureAwait(false);
        return changes as AggressiveCacheDatabaseChanges;
    }
}

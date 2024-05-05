using System;
using Raven.Client.Http;

namespace Raven.Client.Documents.Changes;

internal class AggressiveCacheDatabaseChanges : DatabaseChanges
{
    internal AggressiveCacheDatabaseChanges(RequestExecutor requestExecutor, string databaseName) : base(requestExecutor, databaseName, onDispose: null, nodeTag: null)
    {
    }

    internal override void NotifyAboutReconnection(Exception e)
    {
        NotifyAboutError(e);
    }
}

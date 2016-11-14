using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Client.Data;

namespace Raven.NewClient.Client.Listeners
{
    /// <summary>
    /// Hooks for users that allows you to handle document replication conflicts
    /// </summary>
    public interface IDocumentConflictListener
    {
        bool TryResolveConflict(string key, JsonDocument[] conflictedDocs, out JsonDocument resolvedDocument);
    }
}

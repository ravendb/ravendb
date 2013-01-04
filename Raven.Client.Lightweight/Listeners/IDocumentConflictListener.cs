using Raven.Abstractions.Data;

namespace Raven.Client.Listeners
{
	/// <summary>
	/// Hooks for users that allows you to handle document replication conflicts
	/// </summary>
	public interface IDocumentConflictListener
	{
		bool TryResolveConflict(string key, JsonDocument[] conflictedDocs, out JsonDocument resolvedDocument);
	}
}
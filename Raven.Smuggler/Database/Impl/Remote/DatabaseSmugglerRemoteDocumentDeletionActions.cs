using System.Threading;
using System.Threading.Tasks;

using Raven.Client;

namespace Raven.Smuggler.Database.Impl.Remote
{
	public class DatabaseSmugglerRemoteDocumentDeletionActions : IDatabaseSmugglerDocumentDeletionActions
	{
		private readonly IDocumentStore _store;

		public DatabaseSmugglerRemoteDocumentDeletionActions(IDocumentStore store)
		{
			_store = store;
		}

		public void Dispose()
		{
		}

		public Task WriteDocumentDeletionAsync(string key, CancellationToken cancellationToken)
		{
			return _store.AsyncDatabaseCommands.DeleteAsync(key, null, cancellationToken);
		}
	}
}
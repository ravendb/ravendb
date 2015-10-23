using System.Threading;
using System.Threading.Tasks;

using Raven.Client;

namespace Raven.Smuggler.Database.Remote
{
	public class DatabaseSmugglerRemoteIdentityActions : IDatabaseSmugglerIdentityActions
	{
		private readonly IDocumentStore _store;

		public DatabaseSmugglerRemoteIdentityActions(IDocumentStore store)
		{
			_store = store;
		}

		public void Dispose()
		{
		}

		public Task WriteIdentityAsync(string name, long value, CancellationToken cancellationToken)
		{
			return _store.AsyncDatabaseCommands.SeedIdentityForAsync(name, value, cancellationToken);
		}
	}
}
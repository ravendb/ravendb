using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Indexing;
using Raven.Client;

namespace Raven.Smuggler.Database.Impl.Remote
{
	public class DatabaseSmugglerRemoteIndexActions : IDatabaseSmugglerIndexActions
	{
		private readonly IDocumentStore _store;

		public DatabaseSmugglerRemoteIndexActions(IDocumentStore store)
		{
			_store = store;
		}

		public void Dispose()
		{
		}

		public Task WriteIndexAsync(IndexDefinition index, CancellationToken cancellationToken)
		{
			return _store.AsyncDatabaseCommands.PutIndexAsync(index.Name, index, cancellationToken);
		}
	}
}
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Indexing;
using Raven.Client;

namespace Raven.Smuggler.Database.Remote
{
	public class DatabaseSmugglerRemoteTransformerActions : IDatabaseSmugglerTransformerActions
	{
		private readonly IDocumentStore _store;

		public DatabaseSmugglerRemoteTransformerActions(IDocumentStore store)
		{
			_store = store;
		}

		public void Dispose()
		{
		}

		public Task WriteTransformerAsync(TransformerDefinition transformer, CancellationToken cancellationToken)
		{
			return _store.AsyncDatabaseCommands.PutTransformerAsync(transformer.Name, transformer, cancellationToken);
		}
	}
}
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Util;
using Raven.Client;
using Raven.Client.Document;
using Raven.Json.Linq;

namespace Raven.Smuggler.Database.Impl.Remote
{
	public class DatabaseSmugglerRemoteDocumentActions : IDatabaseSmugglerDocumentActions
	{
		private readonly BulkInsertOperation _bulkInsert;

		public DatabaseSmugglerRemoteDocumentActions(IDocumentStore store)
		{
			_bulkInsert = store.BulkInsert();
		}

		public void Dispose()
		{
			_bulkInsert?.Dispose();
		}

		public Task WriteDocumentAsync(RavenJObject document, CancellationToken cancellationToken)
		{
			_bulkInsert.Store(document);
			return new CompletedTask();
		}
	}
}
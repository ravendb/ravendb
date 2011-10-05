using Raven.Client.Connection.Async;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Indexes
{
	public class IndexesModelLocator : ModelLocatorBase<IndexesModel>
	{
		protected override void Load(DatabaseModel database, IAsyncDatabaseCommands asyncDatabaseCommands, Observable<IndexesModel> observable)
		{
			observable.Value = new IndexesModel(asyncDatabaseCommands);
		}
	}
}
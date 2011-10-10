using Raven.Client.Connection.Async;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Query
{
	public class QueryModelLocator : ModelLocatorBase<QueryModel>
	{
		protected override void Load(IAsyncDatabaseCommands asyncDatabaseCommands, Observable<QueryModel> observable)
		{
			var indexName = GetParamAfter("/query/");
			if (indexName == null)
				return;
			observable.Value = new QueryModel(indexName, asyncDatabaseCommands);
		}
	}
}
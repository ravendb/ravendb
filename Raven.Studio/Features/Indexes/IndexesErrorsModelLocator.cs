using System.Linq;
using Raven.Abstractions.Data;
using Raven.Client.Connection.Async;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Indexes
{
	public class IndexesErrorsModelLocator : ModelLocatorBase<IndexesErrorsModel>
	{
		protected override void Load(IAsyncDatabaseCommands asyncDatabaseCommands, Observable<IndexesErrorsModel> observable)
		{
			string indexName = GetParamAfter("/indexes-errors/");
			var errors = DatabaseModel.Statistics.Value.Errors;
			if (string.IsNullOrEmpty(indexName) == false)
				errors = errors.Where(e => e.Index == indexName).ToArray();
			observable.Value = new IndexesErrorsModel { Errors = errors };
			DatabaseModel.Statistics.PropertyChanged += (sender, args) => observable.Value.OnStatisticsUpdated();
		}
	}
}
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
	public class IndexStatsModel : PageViewModel
	{
		public IndexStatsModel()
		{
			ModelUrl = "/indexstats";
		}

		private string indexName;
		public string IndexName
		{
			get { return indexName; }
			set
			{
				indexName = value;
				DatabaseCommands.GetStatisticsAsync()
					.ContinueOnSuccessInTheUIThread(x =>
					{
						foreach (var index in x.Indexes.Where(index => index.Name == indexName))
						{
							indexStats = index;
							OnPropertyChanged(() => IndexStats);
						}
					});
				OnPropertyChanged(() => IndexName);
			}
		}

		public override void LoadModelParameters(string parameters)
		{
			var urlParser = new UrlParser(parameters);

			IndexName = urlParser.Path.Trim('/');

			DatabaseCommands.GetIndexAsync(IndexName)
				.ContinueOnSuccessInTheUIThread(definition =>
				{
					if (definition == null)
					{
						IndexDefinitionModel.HandleIndexNotFound(IndexName);
						return;
					}
				}).Catch();
		}

		private IndexStats indexStats;
		public IndexStats IndexStats
		{
			get { return indexStats; }
		}
	}
}

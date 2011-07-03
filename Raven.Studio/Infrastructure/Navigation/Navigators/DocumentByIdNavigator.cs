using System.Collections.Generic;
using System.ComponentModel.Composition;
using Raven.Studio.Features.Database;

namespace Raven.Studio.Infrastructure.Navigation.Navigators
{
	[ExportMetadata("Url", @"(?<database>\w+)/(?<page>.*)")]
	[Export(typeof(INavigator))]
	public class DatabaseExplorerNavigator : BaseNavigator
	{
		private readonly DatabaseExplorer databaseExplorer;

		[ImportingConstructor]
		public DatabaseExplorerNavigator(DatabaseExplorer databaseExplorer)
		{
			this.databaseExplorer = databaseExplorer;
		}

		protected override void OnNavigate(Dictionary<string, string> parameters)
		{
			var page = parameters["page"];

			databaseExplorer.SelectedItem = page;
			databaseExplorer.ShowByDisplayName(page);
		}
	}
}
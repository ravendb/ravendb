using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Linq;
using Raven.Studio.Features.Database;

namespace Raven.Studio.Infrastructure.Navigation.Navigators
{
	[ExportMetadata("Url", @"^(?<page>.*)")]
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
			if (string.IsNullOrWhiteSpace(page) || databaseExplorer.AvailableItems
																		.Select(item => item.Metadata.DisplayName)
																		.Contains(page) == false)
				return;
			
			databaseExplorer.SelectedItem = page;
			databaseExplorer.ShowByDisplayName(page);
		}
	}
}
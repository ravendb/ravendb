using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Linq;
using Caliburn.Micro;
using Raven.Studio.Features.Database;

namespace Raven.Studio.Infrastructure.Navigation.Navigators
{
	[NavigatorExport(@"^(?<page>.*)", Index = 20)]
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
			if (string.IsNullOrWhiteSpace(page))
				return;

			var navigateTo = databaseExplorer.AvailableItems
				.Where(item => item.Metadata.DisplayName.Equals(page, StringComparison.InvariantCultureIgnoreCase))
				.FirstOrDefault();

			if (navigateTo == null)
				return;

			var navigateToScreen = (IScreen)navigateTo.Value;
			databaseExplorer.Show(navigateToScreen);
		}
	}
}
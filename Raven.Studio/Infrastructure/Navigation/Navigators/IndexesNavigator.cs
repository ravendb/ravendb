using System.Collections.Generic;
using System.ComponentModel.Composition;
using Raven.Studio.Features.Indexes;
using Raven.Studio.Shell;

namespace Raven.Studio.Infrastructure.Navigation.Navigators
{
	[NavigatorExport(@"^indexes/(?<index>.*)", Index = 40)]
	public class IndexesNavigator : BaseNavigator
	{
		private readonly IShell shellViewModel;
		private readonly BrowseIndexesViewModel browseIndexesViewModel;

		[ImportingConstructor]
		public IndexesNavigator(IShell shellViewModel, BrowseIndexesViewModel browseIndexesViewModel)
		{
			this.shellViewModel = shellViewModel;
			this.browseIndexesViewModel = browseIndexesViewModel;
		}

		protected override void OnNavigate(Dictionary<string, string> parameters)
		{
			var index = parameters["index"];
			if (string.IsNullOrWhiteSpace(index))
				return;

			shellViewModel.DatabaseScreen.Show(browseIndexesViewModel);
			browseIndexesViewModel.SelectIndexByName(index);
		}
	}
}
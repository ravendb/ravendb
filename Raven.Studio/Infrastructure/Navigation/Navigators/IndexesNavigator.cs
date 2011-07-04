using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Linq;
using Raven.Studio.Features.Indexes;

namespace Raven.Studio.Infrastructure.Navigation.Navigators
{
	[NavigatorExport(@"^indexes/(?<index>.*)", Index = 40)]
	public class IndexesNavigator : BaseNavigator
	{
		private readonly BrowseIndexesViewModel browseIndexesViewModel;

		[ImportingConstructor]
		public IndexesNavigator(BrowseIndexesViewModel browseIndexesViewModel)
		{
			this.browseIndexesViewModel = browseIndexesViewModel;
		}

		protected override void OnNavigate(Dictionary<string, string> parameters)
		{
			var index = parameters["index"];
			if (string.IsNullOrWhiteSpace(index))
				return;

			var navigateTo = browseIndexesViewModel.Indexes
				.Where(item => item.Name.Equals(index, StringComparison.InvariantCultureIgnoreCase))
				.FirstOrDefault();

			if (navigateTo == null)
				return;

			browseIndexesViewModel.ActiveIndex = navigateTo;
		}
	}
}
using System.Collections.Generic;
using System.ComponentModel.Composition;
using Raven.Studio.Features.Collections;
using Raven.Studio.Shell;

namespace Raven.Studio.Infrastructure.Navigation.Navigators
{
	[NavigatorExport(@"^collections/(?<collection>.*)", Index = 19)]
	public class CollectionsNavigator : BaseNavigator
	{
		private readonly IShell shellViewModel;
		private readonly CollectionsViewModel collectionsViewModel;

		[ImportingConstructor]
		public CollectionsNavigator(IShell shellViewModel, CollectionsViewModel collectionsViewModel)
		{
			this.shellViewModel = shellViewModel;
			this.collectionsViewModel = collectionsViewModel;
		}

		protected override void OnNavigate(Dictionary<string, string> parameters)
		{
			var collection = parameters["collection"];
			if (string.IsNullOrWhiteSpace(collection))
				return;

			shellViewModel.DatabaseScreen.Show(collectionsViewModel);
			collectionsViewModel.SelectCollectionByName(collection);
		}
	}
}
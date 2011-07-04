using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Linq;
using Raven.Studio.Features.Collections;

namespace Raven.Studio.Infrastructure.Navigation.Navigators
{
	[NavigatorExport(@"^collections/(?<collection>.*)", Index = 19)]
	public class CollectionsNavigator : BaseNavigator
	{
		private readonly CollectionsViewModel collectionsViewModel;

		[ImportingConstructor]
		public CollectionsNavigator(CollectionsViewModel collectionsViewModel)
		{
			this.collectionsViewModel = collectionsViewModel;
		}

		protected override void OnNavigate(Dictionary<string, string> parameters)
		{
			var collection = parameters["collection"];
			if (string.IsNullOrWhiteSpace(collection))
				return;

			var activeCollection = collectionsViewModel.Collections
				.Where(item => item.Name.Equals(collection, StringComparison.InvariantCultureIgnoreCase))
				.FirstOrDefault();

			if (activeCollection == null)
				return;

			collectionsViewModel.ActiveCollection = activeCollection;
		}
	}
}
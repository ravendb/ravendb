using System.Collections.Generic;
using System.ComponentModel.Composition;
using Raven.Studio.Shell;

namespace Raven.Studio.Infrastructure.Navigation.Navigators
{
	[NavigatorExport(@"^home", Index = 10)]
	public class SelectDatabaseNavigator : BaseNavigator
	{
		private readonly IShell shellViewModel;

		[ImportingConstructor]
		public SelectDatabaseNavigator(IShell shellViewModel)
		{
			this.shellViewModel = shellViewModel;
		}

		protected override void OnNavigate(Dictionary<string, string> parameters)
		{
			shellViewModel.Navigation.GoHome();
		}
	}
}
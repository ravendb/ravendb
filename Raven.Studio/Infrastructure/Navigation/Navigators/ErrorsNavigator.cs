using System.Collections.Generic;
using System.ComponentModel.Composition;
using Raven.Studio.Commands;

namespace Raven.Studio.Infrastructure.Navigation.Navigators
{
	[NavigatorExport(@"^errors$", Index = 15)]
	public class ErrorsNavigator : BaseNavigator
	{
		private readonly OpenErrorsScreen errorsScreen;

		[ImportingConstructor]
		public ErrorsNavigator(OpenErrorsScreen errorsScreen)
		{
			this.errorsScreen = errorsScreen;
		}

		protected override void OnNavigate(Dictionary<string, string> parameters)
		{
			errorsScreen.Execute();
		}
	}
}
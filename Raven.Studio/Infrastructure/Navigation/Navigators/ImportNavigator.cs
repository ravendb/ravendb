using System.Collections.Generic;

namespace Raven.Studio.Infrastructure.Navigation.Navigators
{
	[NavigatorExport(@"^import", Index = 11)]
	public class ImportNavigator : INavigator
	{
		public void Navigate(string database, Dictionary<string, string> data)
		{
			throw new System.NotImplementedException();
		}
	}
}
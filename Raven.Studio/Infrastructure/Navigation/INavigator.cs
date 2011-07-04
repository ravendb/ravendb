using System.Collections.Generic;

namespace Raven.Studio.Infrastructure.Navigation
{
	public interface INavigator
	{
		void Navigate(string database, Dictionary<string, string> data);
	}
}
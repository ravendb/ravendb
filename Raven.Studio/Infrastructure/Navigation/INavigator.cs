using System.Collections.Generic;
using System.ComponentModel.Composition;

namespace Raven.Studio.Infrastructure.Navigation
{
	public interface INavigator
	{
		void Navigate(Dictionary<string, string> data);
	}

	[ExportMetadata("Url", @"import")]
	[Export(typeof(INavigator))]
	public class ImportNavigator : INavigator
	{
		public void Navigate(Dictionary<string, string> data)
		{
			
		}
	}
}
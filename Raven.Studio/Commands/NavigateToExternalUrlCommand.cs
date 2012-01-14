using System;
using System.Windows.Browser;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Commands
{
	public class NavigateToExternalUrlCommand : Command
	{
		private string href;

		public override bool CanExecute(object parameter)
		{
			href = parameter as string;
			return href != null;
		}

		public override void Execute(object parameter)
		{
			if (href.StartsWith("http://") == false)
			{
				var ravendbUrl = ApplicationModel.Current.Server.Value.Url;
				href = ravendbUrl + "/" + href;
			}
			HtmlPage.Window.Navigate(new Uri(href, UriKind.Absolute), "_blank");
		}
	}
}
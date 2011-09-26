using System;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Documents
{
	public class NavigateToCommand : Command
	{
		private readonly string href;

		public NavigateToCommand(string href)
		{
			this.href = href;
		}

		public override void Execute(object parameter)
		{
			ApplicationModel.Current.Navigate(new Uri(href, UriKind.Relative));
		}
	}
}
using System;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Commands
{
	public class NavigateToCommand : Command
	{
		public override void Execute(object parameter)
		{
			var href = parameter as string;
			if (href == null)
				return;
			ApplicationModel.Current.Navigate(new Uri(href, UriKind.Relative));
		}
	}
}
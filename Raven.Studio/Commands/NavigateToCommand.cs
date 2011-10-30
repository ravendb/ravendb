using Raven.Studio.Infrastructure;

namespace Raven.Studio.Commands
{
	public class NavigateToCommand : Command
	{
		public override void Execute(object parameter)
		{
			var href = parameter as string;
			if (href == null)
				return;
			UrlUtil.Navigate(href);
		}
	}
}
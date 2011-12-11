using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Commands
{
	public class NavigateToNextPageCommand : Command
	{
		private readonly PagerModel pager;

		public NavigateToNextPageCommand(PagerModel pager)
		{
			this.pager = pager;
			this.pager.PagerChanged += (sender, args) => RaiseCanExecuteChanged();
		}

		public override void Execute(object parameter)
		{
			pager.NavigateToNextPage();
		}

		public override bool CanExecute(object parameter)
		{
			return pager.HasNextPage;
		}
	}
}
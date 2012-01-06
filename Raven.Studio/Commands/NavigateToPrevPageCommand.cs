using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Commands
{
	public class NavigateToPrevPageCommand : Command
	{
		private readonly PagerModel pager;

		public NavigateToPrevPageCommand(PagerModel pager)
		{
			this.pager = pager;
			this.pager.PagerChanged += (sender, args) => RaiseCanExecuteChanged();
		}

		public override void Execute(object parameter)
		{
			pager.NavigateToPrevPage();
		}

		public override bool CanExecute(object parameter)
		{
			return pager.HasPrevPage;
		}
	}
}
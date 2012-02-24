using System.Linq;
using Raven.Studio.Features.Input;
using Raven.Studio.Infrastructure;
using Raven.Studio.Messages;
using Raven.Studio.Models;

namespace Raven.Studio.Commands
{
	public class ResetIndexCommand : ListBoxCommand<IndexItem>
	{
		public override void Execute(object parameter)
		{
			var index = SelectedItems
				.Select(x => x.IndexName)
				.FirstOrDefault();

			AskUser.ConfirmationAsync("Confirm Reset", string.Format("Are you sure that you want to reset this index? ({0})", index))
				.ContinueWhenTrue(() => ResetIndex(index));
		}

		private void ResetIndex(string indexName)
		{
			DatabaseCommands
				.ResetIndexAsync(indexName)
				.ContinueOnSuccessInTheUIThread(() => 
				{
					ApplicationModel.Current.AddNotification(new Notification("Index " + indexName + " successfully reset"));
				})
				.Catch();
		}
	}
}
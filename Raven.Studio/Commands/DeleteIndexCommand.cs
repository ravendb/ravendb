using System.Linq;
using Raven.Studio.Features.Input;
using Raven.Studio.Infrastructure;
using Raven.Studio.Messages;
using Raven.Studio.Models;

namespace Raven.Studio.Commands
{
	public class DeleteIndexCommand : ListBoxCommand<IndexItem>
	{
		public override void Execute(object parameter)
		{
			var index = SelectedItems
				.Select(x => x.IndexName)
				.FirstOrDefault();

			AskUser.ConfirmationAsync("Confirm Delete", string.Format("Are you sure that you want to delete this index? ({0})", index))
				.ContinueWhenTrue(() => DeleteIndex(index));
		}

		private void DeleteIndex(string indexName)
		{
		    DatabaseCommands
		        .DeleteIndexAsync(indexName)
		        .ContinueOnUIThread(t =>
		                                {
		                                    if (t.IsFaulted)
		                                    {
		                                        ApplicationModel.Current.AddNotification(
		                                            new Notification("index " + indexName +
		                                                             " could not be deleted", NotificationLevel.Error,
		                                                             t.Exception));
		                                    }
		                                    else
		                                    {
		                                        ApplicationModel.Current.AddNotification(
		                                            new Notification("Index " + indexName + " successfully deleted"));
		                                        UrlUtil.Navigate("/indexes");
		                                        IndexesModel.GroupedIndexes.Remove(SelectedItems.First());
		                                    }
		                                });
		}
	}
}
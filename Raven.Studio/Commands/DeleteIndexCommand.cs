using System.Collections.Generic;
using System.Linq;
using Raven.Studio.Features.Input;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Commands
{
	public class DeleteIndexCommand : Command
	{
	    private readonly IndexesModel model;

	    public DeleteIndexCommand(IndexesModel model)
	    {
	        this.model = model;
	    }

		public override void Execute(object parameter)
		{
			AskUser.ConfirmationAsync("Confirm Delete", string.Format("Are you sure that you want to delete this index? ({0})", model.ItemSelection.Name))
				.ContinueWhenTrue(() => DeleteIndex(model.ItemSelection.Name));
		}

		private void DeleteIndex(string indexName)
		{
			DatabaseCommands
				.DeleteIndexAsync(indexName)
				.ContinueOnUIThread(t =>
				{
					if (t.IsFaulted)
					{
						ApplicationModel.Current.AddErrorNotification(t.Exception, "index " + indexName + " could not be deleted");
					}
					else
					{
						ApplicationModel.Current.AddInfoNotification("Index " + indexName + " successfully deleted");
						UrlUtil.Navigate("/indexes");

						var deletedItem = model.Indexes.FirstOrDefault(item => item.Name == indexName);
						model.Indexes.Remove(deletedItem);
					}
				});
		}
	}
}
using System.Collections.Generic;
using System.Linq;
using Raven.Studio.Features.Input;
using Raven.Studio.Infrastructure;
using Raven.Studio.Messages;
using Raven.Studio.Models;

namespace Raven.Studio.Commands
{
	public class DeleteIndexCommand : ItemSelectionCommand<IndexItem>
	{
	    private readonly IndexesModel model;

	    public DeleteIndexCommand(IndexesModel model) : base(model.ItemSelection)
	    {
	        this.model = model;
	    }

        protected override bool CanExecuteOverride(IEnumerable<IndexItem> items)
        {
            return items.Any();
        }

	    protected override void ExecuteOverride(IEnumerable<IndexItem> items)
        {
            var index = items
				.Select(x => x.Name)
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
		                                        ApplicationModel.Current.AddErrorNotification(t.Exception,"index " + indexName +
		                                                             " could not be deleted");
		                                    }
		                                    else
		                                    {
		                                        ApplicationModel.Current.AddInfoNotification("Index " + indexName + " successfully deleted");
		                                        UrlUtil.Navigate("/indexes");

		                                        var deletedItem =
		                                            model.GroupedIndexes.OfType<IndexItem>().FirstOrDefault(
		                                                item => item.Name == indexName);

                                                model.GroupedIndexes.Remove(deletedItem);
		                                    }
		                                });
		}
	}
}
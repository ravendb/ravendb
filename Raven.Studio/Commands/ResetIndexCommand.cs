using System.Collections.Generic;
using System.Linq;
using Raven.Studio.Features.Input;
using Raven.Studio.Infrastructure;
using Raven.Studio.Messages;
using Raven.Studio.Models;

namespace Raven.Studio.Commands
{
	public class ResetIndexCommand : ItemSelectionCommand<IndexItem>
	{
	    private readonly IndexesModel model;

	    public ResetIndexCommand(IndexesModel model) : base(model.ItemSelection)
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

			AskUser.ConfirmationAsync("Confirm Reset", string.Format("Are you sure that you want to reset this index? ({0})", index))
				.ContinueWhenTrue(() => ResetIndex(index));
		}

		private void ResetIndex(string indexName)
		{
			DatabaseCommands
				.ResetIndexAsync(indexName)
				.ContinueOnSuccessInTheUIThread(() => ApplicationModel.Current.AddInfoNotification("Index " + indexName + " successfully reset"))
				.Catch();
		}
	}
}
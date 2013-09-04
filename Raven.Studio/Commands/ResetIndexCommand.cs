using System.Collections.Generic;
using System.Linq;
using Raven.Studio.Features.Input;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Commands
{
	public class ResetIndexCommand : Command
	{
		private readonly IndexesModel model;

		public ResetIndexCommand(IndexesModel model)
	    {
		    this.model = model;
	    }

		public override void Execute(object parameter)
		{
			AskUser.ConfirmationAsync("Confirm Reset", string.Format("Are you sure that you want to reset this index? ({0})", model.ItemSelection.Name))
				.ContinueWhenTrue(() => ResetIndex(model.ItemSelection.Name));
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
using System;
using System.Linq;
using Raven.Studio.Infrastructure;
using Raven.Studio.Messages;
using Raven.Studio.Models;

namespace Raven.Studio.Commands
{
	public class PromoteToAutoIndexCommand : ListBoxCommand<IndexItem>
	{
		public override bool CanExecute(object parameter)
		{
			if (base.CanExecute(parameter) == false)
				return false;

			var index = SelectedItems
				.Select(x => x.IndexName)
				.First();

			return index.StartsWith("Temp/", StringComparison.InvariantCultureIgnoreCase);
		}

		public override void Execute(object parameter)
		{
			var index = SelectedItems
				.Select(x => x.IndexName)
				.First();

			ChangeIndexName(index, index.Replace("Temp/", "Auto/"));
		}

		private void ChangeIndexName(string oldIndexName, string newIndexName)
		{
			var model = ((IndexesModel) Context);

			// Check if there is already an index with that name
			var alreadyExists = IndexesModel.GroupedIndexes
				.OfType<IndexItem>()
				.Any(x => x.IndexName == newIndexName);
			if (alreadyExists)
			{
				ApplicationModel.Current.AddNotification(new Notification("Auto index " + newIndexName + " already exists"));
				model.ForceTimerTicked();
				return;
			}

			DatabaseCommands
				.GetIndexAsync(oldIndexName)
				.ContinueOnSuccess(index =>
				                   	{
				                   		index.Name = newIndexName;
				                   		DatabaseCommands.PutIndexAsync(newIndexName, index, false)
				                   			.ContinueOnSuccess(() => DatabaseCommands.DeleteIndexAsync(oldIndexName))
				                   			.ContinueOnSuccessInTheUIThread(() =>
				                   			                                	{
				                   			                                		ApplicationModel.Current.AddNotification(new Notification("Temp index " + oldIndexName + " successfully promoted"));
				                   			                                		model.ForceTimerTicked();
				                   			                                	})
				                   			.Catch();
				                   	});
		}
	}
}
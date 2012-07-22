using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Studio.Infrastructure;
using Raven.Studio.Messages;
using Raven.Studio.Models;

namespace Raven.Studio.Commands
{
    public class PromoteToAutoIndexCommand : ItemSelectionCommand<IndexItem>
	{
        private readonly IndexesModel model;

        public PromoteToAutoIndexCommand(IndexesModel model) : base(model.ItemSelection)
        {
            this.model = model;
        }

        protected override bool CanExecuteOverride(IEnumerable<IndexItem> items)
        {
			var index = items
				.Select(x => x.Name)
				.FirstOrDefault();

            if (index == null)
            {
                return false;
            }

			return index.StartsWith("Temp/", StringComparison.InvariantCultureIgnoreCase);
		}

        protected override void ExecuteOverride(IEnumerable<IndexItem> items)
        {
            var index = items
				.Select(x => x.Name)
				.First();

			ChangeIndexName(index, index.Replace("Temp/", "Auto/"));
		}

		private void ChangeIndexName(string oldIndexName, string newIndexName)
		{
			// Check if there is already an index with that name
			var alreadyExists = model.GroupedIndexes
				.OfType<IndexItem>()
				.Any(x => x.Name == newIndexName);
			if (alreadyExists)
			{
				ApplicationModel.Current.AddWarningNotification("Auto index " + newIndexName + " already exists");
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
				                   			                                		ApplicationModel.Current.AddInfoNotification("Temp index " + oldIndexName + " successfully promoted");
				                   			                                		model.ForceTimerTicked();
				                   			                                	})
				                   			.Catch();
				                   	});
		}
	}
}
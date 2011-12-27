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
			
			var index = Items
				.Select(x => x.IndexName)
				.First();

			return index.StartsWith("Temp/", StringComparison.InvariantCultureIgnoreCase);
		}

		public override void Execute(object parameter)
		{
			var index = Items
				.Select(x => x.IndexName)
				.First();

			ChangeIndexName(index, index.Replace("Temp/", "Auto/"));
		}

		private void ChangeIndexName(string oldIndexName, string newIndexName)
		{
			DatabaseCommands
				.GetIndexAsync(oldIndexName)
				.ContinueOnSuccess(oldIndex =>
				                   	{
				                   		DatabaseCommands.PutIndexAsync(newIndexName, oldIndex, false)
				                   			.ContinueOnSuccessInTheUIThread(() =>
				                   			                                	{
																					ApplicationModel.Current.AddNotification(new Notification("Temp index " + oldIndexName + " successfully promoted"));
																					UrlUtil.Navigate("/indexes");
																					IndexesModel.GroupedIndexes.Remove(Items.First());
				                   			                                		IndexesModel.GroupedIndexes.Add(new IndexItem {IndexName = newIndexName});
				                   			                                	});
										DatabaseCommands.DeleteIndexAsync(oldIndexName);
				                   	});
		}
	}
}
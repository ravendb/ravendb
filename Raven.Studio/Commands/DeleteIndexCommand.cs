using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Commands;
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
			var indexes = Items
				.Select(x => x.IndexName)
				.ToList();

			AskUser.ConfirmationAsync("Confirm Delete", indexes.Count > 1
															? string.Format("Are you sure you want to delete these {0} indexes?", indexes.Count)
															: string.Format("Are you sure that you want to delete this index? ({0})", indexes.First()))
				.ContinueWhenTrue(() => DeleteIndex(indexes))
				.ContinueWhenTrueInTheUIThread(() =>
				{
					var model = (IndexesModel)Context;
					foreach (var index in Items)
					{
						IndexesModel.GroupedIndexes.Remove(index);
					}
				});
		}

		private void DeleteIndex(List<string> indexes)
		{
			var deleteCommandDatas = indexes
			   .Select(id => new DeleteCommandData { Key = id })
			   .Cast<ICommandData>()
			   .ToArray();

			DatabaseCommands.BatchAsync(deleteCommandDatas)
				.ContinueOnSuccessInTheUIThread(() => DeleteIndexesSuccess(indexes));
		}


		private void DeleteIndexesSuccess(IList<string> indexes)
		{
			var notification = indexes.Count > 1
								? string.Format("{0} index were deleted", indexes.Count)
								: string.Format("Index {0} was deleted", indexes.First());
			ApplicationModel.Current.AddNotification(new Notification(notification));
		}
	}
}
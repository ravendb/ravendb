using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Indexes;
using Raven.Studio.Features.Input;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Commands
{
	public class DeleteIndexesCommand : Command
	{
		private readonly IndexesModel model;

		public DeleteIndexesCommand(IndexesModel model)
		{
			this.model = model;
		}

		public override void Execute(object parameter)
		{
			var group = model.SelectedGroup;
			if (group != null)
			{
				var ravenDocumentsByEntityNameIndexName = new RavenDocumentsByEntityName().IndexName;
				AskUser.ConfirmationAsync("Confirm Delete",
					string.Format("Are you sure that you want to delete all indexes in the group {0}?", group.GroupName))
					.ContinueWhenTrue(() => DeleteIndexes(group.Items.Select(item => item.Name).Where(indexName => indexName != ravenDocumentsByEntityNameIndexName)));
			}
			else
			{

				var deleteItems = parameter as string;
				AskUser.ConfirmationAsync("Confirm Delete",
					string.Format("Are you sure that you want to delete all " + deleteItems + " indexes?"))
					.ContinueWhenTrue(() => DeleteIndex(deleteItems));
			}
		}

		private void DeleteIndex(string deleteItems)
		{
			var ravenDocumentsByEntityNameIndexName = new RavenDocumentsByEntityName().IndexName;
			var indexes = (from indexListItem in model.IndexesOfPriority(deleteItems)
				where indexListItem.Name != ravenDocumentsByEntityNameIndexName
				select indexListItem.Name);
				
			
			DeleteIndexes(indexes);
		}

		private void DeleteIndexes(IEnumerable<string> indexes)
		{
			var tasks = (from index in indexes
						 select new { Task = DatabaseCommands.DeleteIndexAsync(index), Name = index }).ToArray();

			Task.Factory.ContinueWhenAll(tasks.Select(x => x.Task).ToArray(), taskslist =>
			{
				foreach (var task in taskslist)
				{
					var indexName = tasks.First(x => x.Task == task).Name;
					if (task.IsFaulted)
					{
						ApplicationModel.Current.AddErrorNotification(task.Exception, "index " + indexName + " could not be deleted");
					}
					else
					{
						ApplicationModel.Current.AddInfoNotification("Index " + indexName + " successfully deleted");
						var deletedItem = model.Indexes.FirstOrDefault(item => item.Name == indexName);
						model.Indexes.Remove(deletedItem);
					}
				}

				UrlUtil.Navigate("/indexes");
			});
		}
	}
}
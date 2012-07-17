using System.Windows.Input;
using Raven.Studio.Commands;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Tasks
{
	public class IndexingTask : TaskModel
	{
		public IndexingTask()
		{
			Name = "Toggle Indexing";
			Description = "Enable and disable indexing";
			//ApplicationModel.Current.Server.Value.SelectedDatabase.Value.AsyncDatabaseCommands.GetIndexingStatus()
			//    .ContinueOnSuccessInTheUIThread(x =>
			//                                        {
			//                                            IndexingStatus = x;
			//                                            TaskDatas.Add(new TaskData("Current Status:", x));
			//                                        });

			IndexingStatus = "Started";
			TaskDatas.Add(new TaskData("Current Status:", IndexingStatus));
		}

		private string indexingStatus;
		public string IndexingStatus
		{
			get { return indexingStatus; }
			set
			{
				indexingStatus = value;
				OnPropertyChanged(() => IndexingStatus);
				OnPropertyChanged(() => Action);
			}
		}

		public override System.Threading.Tasks.Task TimerTickedAsync()
		{
			return ApplicationModel.DatabaseCommands.GetIndexingStatus()
				.ContinueOnSuccessInTheUIThread(item =>
													{
														IndexingStatus = item;
														TaskDatas.Clear();
														TaskDatas.Add(new TaskData("Current Status:", IndexingStatus));

													});
		}

		public override ICommand Action
		{
			get
			{
				if (IndexingStatus == "Indexing")
					return new StopIndexingCommand(line => Execute.OnTheUI(() => Output.Add(line)));
				if (IndexingStatus == "Paused")
					return new StartIndexingCommand(line => Execute.OnTheUI(() => Output.Add(line)));
				return null;
			}
		}
	}
}

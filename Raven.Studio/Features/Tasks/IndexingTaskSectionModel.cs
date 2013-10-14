using System.Windows.Input;
using Raven.Studio.Commands;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Tasks
{
	public class IndexingTaskSectionModel : BasicTaskSectionModel<ToggleIndexingStatusDatabaseTask>
	{
		public IndexingTaskSectionModel()
		{
			Name = "Toggle Indexing";
			Description = "Enable and disable indexing";

			IndexingStatus = "Started";
			TaskDatas.Add(new TaskData("Current Status:", IndexingStatus));

		    AutoAcknowledge = true;
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
			return ApplicationModel.DatabaseCommands.Admin.GetIndexingStatusAsync()
			                       .ContinueOnSuccessInTheUIThread(item =>
			                       {
				                       IndexingStatus = item;
				                       TaskDatas.Clear();
				                       TaskDatas.Add(new TaskData("Current Status:", IndexingStatus));

			                       });
		}

        protected override ToggleIndexingStatusDatabaseTask CreateTask()
        {
            var action = IndexingStatus == "Indexing" ? ToggleIndexAction.TurnOff : ToggleIndexAction.TurnOn;

            return new ToggleIndexingStatusDatabaseTask(action, DatabaseCommands, Database.Value.Name);
        }

        protected override void OnTaskCompleted()
        {
            base.OnTaskCompleted();

            ForceTimerTicked();
        }
	}
}
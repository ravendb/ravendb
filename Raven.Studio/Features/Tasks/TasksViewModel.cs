namespace Raven.Studio.Features.Tasks
{
	using System;
	using System.Collections.Generic;
	using System.ComponentModel.Composition;
	using System.Linq;
	using Caliburn.Micro;
	using Database;
	using Framework;

	[ExportDatabaseScreen("Tasks",Index = 60)]
	public class TasksViewModel : Conductor<ITask>, IDatabaseScreenMenuItem
	{
		readonly IEnumerable<Lazy<ITask, IMenuItemMetadata>> tasks;
		string selectedTask;

		[ImportingConstructor]
		public TasksViewModel([ImportMany] IEnumerable<Lazy<ITask, IMenuItemMetadata>> tasks)
		{
			DisplayName = "Tasks";

			this.tasks = tasks;

			AvailableTasks = tasks.Select(x => x.Metadata.DisplayName).ToList();
		}

		public IList<string> AvailableTasks { get; private set; }

		public string SelectedTask
		{
			get { return selectedTask; }
			set
			{
				selectedTask = value;
				NotifyOfPropertyChange(() => SelectedTask);
				ActivateItem(tasks.First(x => x.Metadata.DisplayName == selectedTask).Value);
			}
		}
	}

	[MetadataAttribute]
	[AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
	public class ExportTaskAttribute : ExportAttribute
	{
		public ExportTaskAttribute(string displayName) : base(typeof (ITask)) { DisplayName = displayName; }

		public string DisplayName { get; private set; }
		public int Index { get; set; }
	}

	public interface ITask
	{
	}
}
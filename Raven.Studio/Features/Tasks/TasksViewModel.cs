namespace Raven.Studio.Features.Tasks
{
	using System;
	using System.Collections.Generic;
	using System.ComponentModel.Composition;
	using System.Linq;
	using Caliburn.Micro;
	using Database;

	public class TasksViewModel : Conductor<ITask>, IDatabaseScreenMenuItem
	{
		readonly IEnumerable<Lazy<ITask, ITaskMetadata>> tasks;
		string selectedTask;

		[ImportingConstructor]
		public TasksViewModel([ImportMany] IEnumerable<Lazy<ITask, ITaskMetadata>> tasks)
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

		public int Index { get { return 60; } }
	}

	[MetadataAttribute]
	[AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
	public class ExportTaskAttribute : ExportAttribute
	{
		public ExportTaskAttribute(string displayName) : base(typeof (ITask)) { DisplayName = displayName; }

		public string DisplayName { get; private set; }
	}

	public interface ITask
	{
	}

	public interface ITaskMetadata
	{
		string DisplayName { get; }
	}
}
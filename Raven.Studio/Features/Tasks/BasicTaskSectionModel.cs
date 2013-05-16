using System.Linq;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Tasks
{
	public abstract class BasicTaskSectionModel<T> : TaskSectionModel<T> where T : DatabaseTask
	{
		public BasicTaskSectionModel()
		{
			TaskInputs = new BindableCollection<TaskUIObject>(x => x.Name);
			TaskDatas = new BindableCollection<TaskData>(x => x.Name);
		}

		public BindableCollection<TaskUIObject> TaskInputs { get; set; }
		public BindableCollection<TaskData> TaskDatas { get; set; }
		private const int PixelsPerLetter = 7;

		public int LongestInput
		{
			get
			{
				var taskInput = TaskInputs.OrderByDescending(input => input.Name.Length).FirstOrDefault();
				if (taskInput != null)
					return taskInput.Name.Length * PixelsPerLetter;
				return 0;
			}
		}
	}

	public class TaskCheckBox : TaskUIObject
	{
		public TaskCheckBox(string name, bool value)
			: base(name, value)
		{
			Name = name;
		}
	}

	public class TaskInput : TaskUIObject
	{
		public TaskInput(string name, string value)
			: base(name, value)
		{
			Name = name;
		}
	}

	public class TaskData : NotifyPropertyChangedBase
	{
		public TaskData(string name, string value)
		{
			Name = name;
			Value = value;
		}

		private string name;
		public string Name
		{
			get { return name; }
			set
			{
				name = value;
				OnPropertyChanged(() => Name);
			}
		}

		private string value;

		public string Value
		{
			get { return value; }
			set { this.value = value; OnPropertyChanged(() => Value); }
		}
	}

	public abstract class TaskUIObject : NotifyPropertyChangedBase
	{
		public TaskUIObject(string name, object value)
		{
			Name = name;
			Value = value;
		}

		public string name;
		public string Name
		{
			get { return name; }
			set { name = value; OnPropertyChanged(() => Name); }
		}

		private object value;
		public object Value
		{
			get { return this.value; }
			set { this.value = value; OnPropertyChanged(() => Value); }
		}
	}
}

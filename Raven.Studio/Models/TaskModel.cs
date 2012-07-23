using System.ComponentModel;
using System.Windows.Input;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
	public enum TaskStatus
	{
		DidNotStart,
		Started,
		Ended
	}

	public abstract class TaskModel : Model
	{
		public TaskModel()
		{
			Output = new BindableCollection<string>(x => x);
			TaskInputs = new BindableCollection<TaskInput>(x => x.Name);
			TaskDatas = new BindableCollection<TaskData>(x => x.Name);
			TaskStatus = TaskStatus.DidNotStart;
		}

		private string name;
		public string Name
		{
			get { return name; }
			set { name = value; OnPropertyChanged(() => Name); }
		}

	    public string IconResource
	    {
	        get { return iconResource; }
            set { iconResource = value; OnPropertyChanged(() => IconResource); }
	    }

		public string Description { get; set; }

		private TaskStatus taskStatus;
	    private string iconResource;
	    public TaskStatus TaskStatus
		{
			get { return taskStatus; }
			set
			{
				taskStatus = value;
				OnPropertyChanged(() => TaskStatus);
			}
		}

		public BindableCollection<string> Output { get; set; }
		public BindableCollection<TaskInput> TaskInputs { get; set; }
		public BindableCollection<TaskData> TaskDatas { get; set; }

		public abstract ICommand Action { get; }
	}

	public class TaskInput : NotifyPropertyChangedBase
	{
		public TaskInput(string name, string value)
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
}
using System.Collections.Generic;
using System.Windows.Input;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
	public abstract class TaskModel : ViewModel
	{
		public TaskModel()
		{
			Output = new BindableCollection<string>(x => x);
			TaskInputs = new BindableCollection<TaskInput>(x => x.Name);
		}

		private string name;
		public string Name
		{
			get { return name; }
			set { name = value; OnPropertyChanged(); }
		}

		public string Description { get; set; }

		public BindableCollection<string> Output { get; set; }
		public BindableCollection<TaskInput> TaskInputs { get; set; }

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
			set { name = value; OnPropertyChanged(); }
		}

		private string value;
		public string Value
		{
			get { return value; }
			set { this.value = value; OnPropertyChanged(); }
		}
	}
}
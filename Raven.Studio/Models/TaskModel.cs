using System;
using System.Collections.Generic;
using System.Net;
using System.Windows.Input;
using Raven.Studio.Infrastructure;
using System.Linq;
using Raven.Abstractions.Extensions;

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
		private const int PixelsPerLetter = 7;

		public Observable<bool> CanExecute { get; set; }
		public bool StatusBarActive
		{
			get { return TaskStatus == TaskStatus.Started; }
		} 

		public TaskModel()
		{
			Output = new BindableCollection<string>(x => x);
			CanExecute = new Observable<bool>
			{
				Value = true
			};

			TaskInputs = new BindableCollection<TaskUIObject>(x => x.Name);
			TaskDatas = new BindableCollection<TaskData>(x => x.Name);
			TaskStatus = TaskStatus.DidNotStart;
		}

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

		public void ReportError(Exception exception)
		{
			var aggregate = exception as AggregateException;
			if (aggregate != null)
				exception = aggregate.ExtractSingleInnerException();

			var objects = new List<object>();
			var webException = exception as WebException;
			if (webException != null)
			{
				var httpWebResponse = webException.Response as HttpWebResponse;
				if (httpWebResponse != null)
				{
					var stream = httpWebResponse.GetResponseStream();
					if (stream != null)
					{
						objects = ApplicationModel.ExtractError(stream, httpWebResponse);
					}
				}
			}

			if(objects.Count == 0)
				Output.Add("Error: " + exception.Message);
			else
			{
				foreach (var msg in objects)
				{
					if(!string.IsNullOrWhiteSpace(msg.ToString()))
						Output.Add("Error: " + msg);
				}
			}
		}

		public void ReportError(string errorMsg)
		{
			Output.Add("Error: " + errorMsg);
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
				OnPropertyChanged(() => StatusBarActive);
			}
		}

		public BindableCollection<string> Output { get; set; }
		public BindableCollection<TaskUIObject> TaskInputs { get; set; }
		public BindableCollection<TaskData> TaskDatas { get; set; }

		public abstract ICommand Action { get; }
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

    public class TaskCheckBox : TaskUIObject
    {
        public TaskCheckBox(string name, bool value) : base(name, value)
        {
            Name = name;
        }
    }

    public class TaskInput : TaskUIObject
	{
		public TaskInput(string name, string value) : base(name, value)
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
}
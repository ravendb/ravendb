using System;
using System.Collections.Generic;
using System.Net;
using System.Windows.Input;
using Raven.Abstractions.Extensions;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
	public enum TaskStatus
	{
		DidNotStart,
		Started,
		Ended
	}

	public abstract class TaskSectionModel : ViewModel
	{
		public string SectionName { get { return Name; } }

		public Observable<bool> CanExecute { get; set; }
		public bool StatusBarActive
		{
			get { return TaskStatus == TaskStatus.Started; }
		}

		public TaskSectionModel()
		{
			Output = new BindableCollection<string>(x => x);
			CanExecute = new Observable<bool>
			{
				Value = true
			};
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

			if (objects.Count == 0)
				Output.Add("Error: " + exception.Message);
			else
			{
				foreach (var msg in objects)
				{
					if (!string.IsNullOrWhiteSpace(msg.ToString()))
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
		public abstract ICommand Action { get; }
	}
}

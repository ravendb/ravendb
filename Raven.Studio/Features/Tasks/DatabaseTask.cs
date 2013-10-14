using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;
using Raven.Abstractions.Extensions;
using Raven.Client.Connection.Async;
using Raven.Studio.Infrastructure;
using Raven.Studio.Messages;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Tasks
{
    public class DatabaseTaskOutput
    {
        public DatabaseTaskOutput(string message, OutputType outputType)
        {
            Message = message;
            OutputType = outputType;
        }

        public OutputType OutputType { get; private set; }
        public string Message { get; private set; }
    }

    public enum OutputType
    {
        Information,
        Error
    }

    public enum DatabaseTaskStatus
    {
        Idle,
        Running,
        Completed,
    }

    public enum DatabaseTaskOutcome
    {
        Succesful,
        Error,
        Abandoned
    }

    public abstract class DatabaseTask
    {
        private readonly IAsyncDatabaseCommands databaseCommands;
        private readonly string taskName;
        private readonly string databaseName;
        private DatabaseTaskStatus status;

        public event EventHandler<EventArgs> StatusChanged;
        public event EventHandler<OutputEventArgs> MessageOutput;

        protected virtual void OnMessageOutput(OutputEventArgs e)
        {
            EventHandler<OutputEventArgs> handler = MessageOutput;
            if (handler != null) handler(this, e);
        }

        protected virtual void OnStatusChanged()
        {
            EventHandler<EventArgs> handler = StatusChanged;
            if (handler != null) handler(this, EventArgs.Empty);
        }

        public DatabaseTask(IAsyncDatabaseCommands databaseCommands, string taskName, string databaseName)
        {
            this.databaseCommands = databaseCommands;
            this.taskName = taskName;
            this.databaseName = databaseName;
            OutputItems = new ObservableCollection<DatabaseTaskOutput>();
        }

        public DatabaseTaskStatus Status
        {
            get { return status; }
            private set
            {
                status = value;
                OnStatusChanged();
            }
        }

        public DatabaseTaskOutcome? Outcome { get; private set; }

        public IEnumerable<DatabaseTaskOutput> Output { get { return OutputItems; } } 

        private ObservableCollection<DatabaseTaskOutput> OutputItems { get; set; }

        protected IAsyncDatabaseCommands DatabaseCommands
        {
            get { return databaseCommands; }
        }

        protected string DatabaseName
        {
            get { return databaseName; }
        }

	    private Exception innderException = null;
        public async Task<DatabaseTaskOutcome> Run()
        {
            Status = DatabaseTaskStatus.Running;
            var started = DateTime.UtcNow.Ticks;

            try
            {
                Outcome = await RunImplementation();
            }
            catch (Exception ex)
            {
				Outcome = DatabaseTaskOutcome.Error;
	            OnError();
                ReportError(ex);
            }
            finally
            {
                var completed = DateTime.UtcNow.Ticks;
                var elapsed = new TimeSpan(completed - started);
                Report(string.Format("Task completed in {0:g}", elapsed));

                Status = DatabaseTaskStatus.Completed;

                if ((Outcome ?? DatabaseTaskOutcome.Abandoned) == DatabaseTaskOutcome.Succesful)
                {
                    ApplicationModel.Current.AddInfoNotification("Task " + taskName + " has completed for Database " +
                                                                 DatabaseName);
                }
                else if ((Outcome ?? DatabaseTaskOutcome.Abandoned) == DatabaseTaskOutcome.Error)
                {
					
                    ApplicationModel.Current.AddNotification(new Notification("Task " + taskName + " has failed for Database " +
                                                                 DatabaseName, NotificationLevel.Error, innderException));
                }
            }

            return Outcome.Value;
        }

        protected abstract Task<DatabaseTaskOutcome> RunImplementation();

        protected void Report(string info, OutputType type = OutputType.Information)
        {
            Execute.OnTheUI(() =>
            {
                var lines = info.Split(new[] {Environment.NewLine}, StringSplitOptions.None);
                OutputItems.AddRange(lines.Select(l => new DatabaseTaskOutput(l, type)));
                OnMessageOutput(new OutputEventArgs() { Message = info, OutputType = type });
            });
        }

	    public abstract void OnError();

        private void ReportError(Exception exception)
        {
            var aggregate = exception as AggregateException;
            if (aggregate != null)
                exception = aggregate.ExtractSingleInnerException();

	        innderException = exception;

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
	                    try
	                    {
							objects = ApplicationModel.ExtractError(stream, httpWebResponse);
	                    }
	                    catch (Exception)
	                    {

	                    }
                    }
                }
            }

            if (objects.Count == 0)
			{
                Report(exception.Message, OutputType.Error);
			}
            else
            {
				innderException = new Exception(string.Join(Environment.NewLine, objects));
                foreach (var msg in objects)
                {
                    string value = msg.ToString();
                    if (!string.IsNullOrWhiteSpace(value))
                        Report(value, OutputType.Error);
                }
            }
        }

        protected void ReportError(string errorMsg)
        {
            Report(errorMsg, OutputType.Error);
        }
    }

    public class OutputEventArgs : EventArgs
    {
        public string Message { get; set; }
        public OutputType OutputType { get; set; }
    }
}

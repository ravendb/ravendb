using System;
using System.Collections.Generic;
using System.Net;
using System.Windows.Input;
using Microsoft.Expression.Interactivity.Core;
using Raven.Abstractions.Extensions;
using Raven.Studio.Features.Tasks;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
    public enum TaskStatus
    {
        DidNotStart,
        Started,
        Ended
    }

    public abstract class TaskSectionModel<T> : ViewModel where T : DatabaseTask
    {
        public string SectionName
        {
            get { return Name; }
        }

        public bool CanExecute
        {
            get
            {
                return Task == null || Task.Status == DatabaseTaskStatus.Idle ||
                       Task.Status == DatabaseTaskStatus.Completed;
            }
        }

        public bool IsStatusViewVisible
        {
            get
            {
                return IsTaskRunning || IsTaskCompleted; 
            }
        }

        public bool IsTaskRunning
        {
            get { return Task != null && Task.Status == DatabaseTaskStatus.Running; }
        }

        public bool IsTaskCompleted
        {
            get
            {
                return Task != null && Task.Status == DatabaseTaskStatus.Completed; 
            }
        }

        public TaskSectionModel()
        {
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

        public string IconResource
        {
            get { return iconResource; }
            set
            {
                iconResource = value;
                OnPropertyChanged(() => IconResource);
            }
        }

        public string Description { get; set; }

        private string iconResource;
        private ActionCommand actionCommand;
        private ICommand acknowledgeCompletion;

        protected bool AutoAcknowledge { get; set; }

        protected T Task
        {
            get
            {
                DatabaseTask task;
                return (T) (PerDatabaseState.ActiveTasks.TryGetValue(typeof (T), out task) ? task : null);
            }
            set
            {
                PerDatabaseState.ActiveTasks[typeof (T)] = value;

                OnPropertyChanged(() => CanExecute);
                OnPropertyChanged(() => IsTaskCompleted);
                OnPropertyChanged(() => IsTaskRunning);
                OnPropertyChanged(() => IsStatusViewVisible);
                OnPropertyChanged(() => Output);
            }
        }

        public IEnumerable<DatabaseTaskOutput> Output
        {
            get { return Task == null ? new DatabaseTaskOutput[0] : Task.Output; }
        }

        public ICommand AcknowledgeCompletion
        {
            get { return acknowledgeCompletion ?? (acknowledgeCompletion = new ActionCommand(HandleAcknowledgeCompletion)); }
        }

        private void HandleAcknowledgeCompletion()
        {
            Task = null;
        }

        public ICommand Action
        {
            get { return actionCommand ?? (actionCommand = new ActionCommand(HandleExecute)); }
        }

        private void HandleExecute()
        {
            if (!CanExecute)
            {
                return;
            }

            var task = CreateTask();

            Task = task;

            task.StatusChanged += HandleTaskStatusChanged;

            task.Run();
        }

        protected override void OnViewLoaded()
        {
            var task = Task;
            if (task != null)
            {
                if (Task.Status == DatabaseTaskStatus.Completed && (Task.Outcome.Value == DatabaseTaskOutcome.Abandoned || Task.Outcome.Value == DatabaseTaskOutcome.Succesful && AutoAcknowledge))
                {
                    Task = null;
                }
                else
                {
                    Task.StatusChanged += HandleTaskStatusChanged;

                    OnPropertyChanged(() => CanExecute);
                    OnPropertyChanged(() => IsTaskCompleted);
                    OnPropertyChanged(() => IsStatusViewVisible);
                    OnPropertyChanged(() => Output);
                }
            }
        }

        protected override void OnViewUnloaded()
        {
            var task = Task;
            if (task != null)
            {
                Task.StatusChanged -= HandleTaskStatusChanged;
            }
        }

        private void HandleTaskStatusChanged(object sender, EventArgs e)
        {
            OnPropertyChanged(() => CanExecute);
            OnPropertyChanged(() => IsTaskCompleted);
            OnPropertyChanged(() => IsTaskRunning);
            OnPropertyChanged(() => IsStatusViewVisible);
            OnTaskCompleted();
        }

        protected abstract T CreateTask();

        protected virtual void OnTaskCompleted()
        {
            if (Task.Status == DatabaseTaskStatus.Completed && (Task.Outcome.Value == DatabaseTaskOutcome.Abandoned ||
                                                                (Task.Outcome.Value == DatabaseTaskOutcome.Succesful &&
                                                                 AutoAcknowledge)))
            {
                Task = null;
            }
        }
    }
}

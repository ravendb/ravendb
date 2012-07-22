using System;
using System.Windows.Input;
using Raven.Client.Connection.Async;
using Raven.Studio.Models;

namespace Raven.Studio.Infrastructure
{
	public abstract class Command : ICommand
	{
		public virtual bool CanExecute(object parameter)
		{
			return shouldExecute;
		}

		private bool shouldExecute = true;
		protected bool ShouldExecute
		{
			get { return shouldExecute; }
			set
			{
				shouldExecute = value;
				RaiseCanExecuteChanged();
			}
		}

		public abstract void Execute(object parameter);

		public event EventHandler CanExecuteChanged = delegate { };

		public void RaiseCanExecuteChanged()
		{
			EventHandler handler = CanExecuteChanged;
			if (handler != null) handler(this, EventArgs.Empty);
		}

		public IAsyncDatabaseCommands DatabaseCommands
		{
			get { return ApplicationModel.DatabaseCommands; }
		}

		public static void ExecuteCommand(ICommand command, object param = null)
		{
			if (command.CanExecute(param))
				command.Execute(param);
		}
	}
}

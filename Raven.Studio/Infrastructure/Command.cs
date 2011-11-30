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
			return true;
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
	}
}
using System;
using System.Windows.Input;

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

		protected void OnCanExecuteChanged()
		{
			EventHandler handler = CanExecuteChanged;
			if (handler != null) handler(this, EventArgs.Empty);
		}
	}
}
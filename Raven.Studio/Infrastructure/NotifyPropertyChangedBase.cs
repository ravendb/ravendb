using System.ComponentModel;
using System.Diagnostics;
using System.Windows;

namespace Raven.Studio.Infrastructure
{
	public class NotifyPropertyChangedBase : INotifyPropertyChanged
	{
		private event PropertyChangedEventHandler PropertyChangedInternal;
		public event PropertyChangedEventHandler PropertyChanged
		{
			add
			{
				var dispatcher = Deployment.Current.Dispatcher;
				PropertyChangedInternal += (sender, args) =>
				{
					if (dispatcher.CheckAccess())
						value(sender, args);
					else
						dispatcher.InvokeAsync(() => value(sender, args));
				};
			}
			remove { throw new System.NotImplementedException(); }
		}

		public void OnPropertyChanged()
		{
			var handler = PropertyChangedInternal;
			if (handler == null)
				return;

			var stackTrace = new StackTrace();
			var name = stackTrace.GetFrame(1).GetMethod().Name.Substring(4);

			handler(this, new PropertyChangedEventArgs(name));
		}
	}
}
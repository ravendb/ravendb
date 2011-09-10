using System;
using System.ComponentModel;
using System.Diagnostics;
using System.Windows;
using System.Windows.Threading;
using System.Linq;

namespace Raven.Studio.Infrastructure
{
	public class NotifyPropertyChangedBase : INotifyPropertyChanged
	{
		private event PropertyChangedEventHandler PropertyChangedInternal;
		public event PropertyChangedEventHandler PropertyChanged
		{
			add
			{
				var state = new EventState(value);
				PropertyChangedInternal += state.Invoke;
			}
			remove
			{
				EventState firstOrDefault = PropertyChangedInternal.GetInvocationList()
					.Select(x => ((EventState) x.Target))
					.FirstOrDefault(x => x.Value == value);
				
				if (firstOrDefault == null)
					return;

				PropertyChangedInternal -= firstOrDefault.Invoke;
			}
		}

		private class EventState
		{
			public PropertyChangedEventHandler Value { get; private set; }
			private readonly Dispatcher dispatcher = Deployment.Current.Dispatcher;

			public EventState(PropertyChangedEventHandler value)
			{
				this.Value = value;
			}

			public void Invoke(object sender, PropertyChangedEventArgs e)
			{
				if (dispatcher.CheckAccess())
					Value(sender, e);
				else
					dispatcher.InvokeAsync(() => Value(sender, e));
			
			}
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
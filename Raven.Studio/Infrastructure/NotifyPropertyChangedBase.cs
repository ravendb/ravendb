using System;
using System.ComponentModel;
using System.Diagnostics;
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
				var state = new EventState(this, value);
				PropertyChangedInternal += state.Invoke;
			}
			remove
			{
				EventState firstOrDefault = PropertyChangedInternal.GetInvocationList()
					.Select(x => ((EventState) x.Target))
					.FirstOrDefault(x => ReferenceEquals(x.Value.Target, value));
				
				if (firstOrDefault == null)
					return;

				PropertyChangedInternal -= firstOrDefault.Invoke;
			}
		}

		private class EventState
		{
			private readonly NotifyPropertyChangedBase parent;
			public WeakReference Value { get; private set; }

			public EventState(NotifyPropertyChangedBase parent, PropertyChangedEventHandler value)
			{
				this.parent = parent;
				this.Value = new WeakReference(value);
			}

			public void Invoke(object sender, PropertyChangedEventArgs e)
			{
				var progressChangedEventHandler = Value.Target as PropertyChangedEventHandler;

				if (progressChangedEventHandler == null)
				{
					parent.PropertyChangedInternal -= Invoke;
					return;
				}

				Execute.OnTheUI(() => progressChangedEventHandler(sender, e));
			}
		}

		protected void OnEverythingChanged()
		{
			var handler = PropertyChangedInternal;
			if (handler == null)
				return;

			handler(this, new PropertyChangedEventArgs(""));
		}

		protected void OnPropertyChanged()
		{
			var stackTrace = new StackTrace();
			var name = stackTrace.GetFrame(1).GetMethod().Name.Substring(4);

			OnPropertyChanged(name);
		}

		protected void OnPropertyChanged(string name)
		{
			var handler = PropertyChangedInternal;
			if (handler == null)
				return;

			handler(this, new PropertyChangedEventArgs(name));
		}
	}
}
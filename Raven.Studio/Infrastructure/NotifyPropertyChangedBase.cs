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

			public EventState(PropertyChangedEventHandler value)
			{
				this.Value = value;
			}

			public void Invoke(object sender, PropertyChangedEventArgs e)
			{
				Execute.OnTheUI(() => Value(sender, e));
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
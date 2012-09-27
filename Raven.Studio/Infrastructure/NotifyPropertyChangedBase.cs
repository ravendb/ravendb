using System;
using System.ComponentModel;
using System.Linq;
using System.Linq.Expressions;

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
				var firstOrDefault = PropertyChangedInternal.GetInvocationList()
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
				Value = value;
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

		protected void OnPropertyChanged<T>(Expression<Func<T>> path)
		{
			var member = (MemberExpression) path.Body;
			OnPropertyChanged(member.Member.Name);
		}

		private void OnPropertyChanged(string name)
		{
			var handler = PropertyChangedInternal;
			if (handler == null)
				return;

			handler(this, new PropertyChangedEventArgs(name));
		}
	}
}
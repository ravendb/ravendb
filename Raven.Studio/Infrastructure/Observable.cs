using System;
using System.ComponentModel;
using System.Windows;
using System.Windows.Markup;

namespace Raven.Studio.Infrastructure
{
	[ContentProperty("Value")]
	public class Observable<T> : NotifyPropertyChangedBase , IObservable 
		where T : class
	{
		private T value;

		public T Value
		{
			get { return value; }
			set
			{
				this.value = value;
				OnPropertyChanged();
				var onActions = actions;
				actions = null;
				if (onActions == null) 
					return;

				var dispatcher = Deployment.Current.Dispatcher;
				if (dispatcher.CheckAccess())
					onActions();
				else
					dispatcher.InvokeAsync(onActions);
			}
		}

		object IObservable.Value
		{
			get { return value; }
		}

		private event Action actions;

		public void RegisterOnce(Action act)
		{

			actions += act;
		}
	}

	public interface IObservable : INotifyPropertyChanged
	{
		object Value { get; }
	}
}
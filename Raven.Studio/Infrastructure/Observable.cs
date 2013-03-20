using System;
using System.ComponentModel;
using System.Windows.Markup;

namespace Raven.Studio.Infrastructure
{
	[ContentProperty("Value")]
	public class Observable<T> : NotifyPropertyChangedBase , IObservable 
	{
		private readonly IObservable parent;
		private readonly Func<object, T> valueExtractor;
		private T value;

		public Observable(IObservable parent, Func<object, T> valueExtractor)
		{
			this.parent = parent;
			this.valueExtractor = valueExtractor;
			parent.PropertyChanged += (sender, args) => GetValueFromParent();
			GetValueFromParent();
		}

		public Observable()
		{
			
		}

		private void GetValueFromParent()
		{
			var parentValue = parent.Value;
			Value = parentValue != null ? valueExtractor(parentValue) : default(T);
		}

		public T Value
		{
			get { return value; }
			set
			{
				if (ReferenceEquals(this.value, value))
					return;
				this.value = value;
				OnPropertyChanged(() => Value);
				var onActions = actions;
				actions = null;
				if (onActions == null) 
					return;

				Execute.OnTheUI(onActions);
			}
		}

		object IObservable.Value
		{
			get { return value; }
			set { Value = (T)value; }
		}

		private event Action actions;

		public void RegisterOnce(Action act)
		{
			actions += act;
		}
	}

	public interface IObservable : INotifyPropertyChanged
	{
		object Value { get; set; }
	}
}
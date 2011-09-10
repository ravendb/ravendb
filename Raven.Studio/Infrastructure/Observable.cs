using System.ComponentModel;
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
			}
		}

		object IObservable.Value
		{
			get { return value; }
		}
	}

	public interface IObservable : INotifyPropertyChanged
	{
		object Value { get; }
	}
}
using System.Windows.Markup;

namespace Raven.Studio.Infrastructure
{
	[ContentProperty("Value")]
	public class Observable<T> : NotifyPropertyChangedBase 
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
	}
}
using Caliburn.Micro;

namespace Raven.Studio.Features.Collections
{
	public class CollectionViewModel : PropertyChangedBase
	{
		private string name;
		public string Name
		{
			get { return name; }
			set
			{
				name = value;
				NotifyOfPropertyChange(() => Name);
			}
		}

		private int count;
		public int Count
		{
			get { return count; }
			set
			{
				count = value;
				NotifyOfPropertyChange(() => Count);
			}
		}
	}
}
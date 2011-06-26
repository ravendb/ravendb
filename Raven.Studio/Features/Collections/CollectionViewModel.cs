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

		private long count;
		public long Count
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
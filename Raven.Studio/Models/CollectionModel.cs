using System.Windows.Media;
using Raven.Studio.Features.Documents;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
	public class CollectionModel : PageViewModel
	{
		Brush fill;
		public Brush Fill
		{
			get { return fill ?? (fill = TemplateColorProvider.Instance.ColorFrom(Name)); }
		}

		private string name;
		public string Name
		{
			get { return name; }
			set { name = value; OnPropertyChanged(() => Name);}
		}

		private int count;
		public int Count
		{
			get { return count; }
			set { count = value; OnPropertyChanged(() => Count);}
		}
	}
}
namespace Raven.Studio.Indexes.Browse
{
	using System.Windows.Controls;

	public partial class BrowseIndexesView
	{
		public BrowseIndexesView()
		{
			InitializeComponent();
		}

		void SearchBoxTextChanged(object sender, TextChangedEventArgs e)
		{
			((BrowseIndexesViewModel) DataContext).Filter = searchBox.Text;
		}
	}
}
namespace Raven.ManagementStudio.UI.Silverlight.Indexes.Browse
{
	public partial class BrowseIndexesView
    {
        public BrowseIndexesView()
        {
            InitializeComponent();
        }

        private void SearchBoxTextChanged(object sender, System.Windows.Controls.TextChangedEventArgs e)
        {
            ((BrowseIndexesViewModel)DataContext).Filter = searchBox.Text;
        }
    }
}

namespace Raven.ManagementStudio.UI.Silverlight.Plugins.Indexes.Browse
{
    public partial class BrowseIndexesScreenView
    {
        public BrowseIndexesScreenView()
        {
            InitializeComponent();
        }

        private void SearchBoxTextChanged(object sender, System.Windows.Controls.TextChangedEventArgs e)
        {
            ((BrowseIndexesScreenViewModel)DataContext).Filter = searchBox.Text;
        }
    }
}

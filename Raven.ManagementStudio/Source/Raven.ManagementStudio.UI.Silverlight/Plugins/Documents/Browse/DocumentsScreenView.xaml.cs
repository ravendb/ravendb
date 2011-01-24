namespace Raven.ManagementStudio.UI.Silverlight.Plugins.Documents.Browse
{
    public partial class DocumentsScreenView
    {
        public DocumentsScreenView()
        {
            InitializeComponent();
        }

        private void DocumentIdKeyDown(object sender, System.Windows.Input.KeyEventArgs e)
        {
            if (e.Key == System.Windows.Input.Key.Enter)
            {
                ((DocumentsScreenViewModel)DataContext).ShowDocument(documentId.Text);
            }
        }
    }
}

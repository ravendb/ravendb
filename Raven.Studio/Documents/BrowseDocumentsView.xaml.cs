namespace Raven.Studio.Documents
{
	using System.Windows.Input;

	public partial class BrowseDocumentsView
	{
		public BrowseDocumentsView()
		{
			InitializeComponent();
		}

		void DocumentIdKeyDown(object sender, KeyEventArgs e)
		{
			if (e.Key == Key.Enter)
			{
				((BrowseDocumentsViewModel) DataContext).ShowDocument(documentId.Text);
			}
		}
	}
}
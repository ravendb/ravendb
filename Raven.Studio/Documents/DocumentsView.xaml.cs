namespace Raven.Studio.Documents
{
	using System.Windows.Input;

	public partial class DocumentsView
	{
		public DocumentsView()
		{
			InitializeComponent();
		}

		void DocumentIdKeyDown(object sender, KeyEventArgs e)
		{
			if (e.Key == Key.Enter)
			{
				((DocumentsViewModel) DataContext).ShowDocument(documentId.Text);
			}
		}
	}
}
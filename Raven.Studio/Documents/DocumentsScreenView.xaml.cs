namespace Raven.Studio.Documents
{
	using System.Windows.Input;

	public partial class DocumentsScreenView
	{
		public DocumentsScreenView()
		{
			InitializeComponent();
		}

		void DocumentIdKeyDown(object sender, KeyEventArgs e)
		{
			if (e.Key == Key.Enter)
			{
				((DocumentsScreenViewModel) DataContext).ShowDocument(documentId.Text);
			}
		}
	}
}
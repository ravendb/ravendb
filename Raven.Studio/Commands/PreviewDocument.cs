namespace Raven.Studio.Commands
{
	using System.ComponentModel.Composition;
	using Caliburn.Micro;
	using Features.Database;
	using Features.Documents;

	public class PreviewDocument
	{
		readonly IWindowManager windows;

		[ImportingConstructor]
		public PreviewDocument(IWindowManager windows)
		{
			this.windows = windows;
		}

		public void Execute(EditDocumentViewModel document)
		{
			windows.ShowDialog(document, "Preview");
		}
	}
}
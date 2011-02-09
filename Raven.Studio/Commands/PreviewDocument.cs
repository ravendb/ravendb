namespace Raven.Studio.Commands
{
	using System.ComponentModel.Composition;
	using Caliburn.Micro;
	using Database;

	public class PreviewDocument
	{
		readonly IWindowManager windows;

		[ImportingConstructor]
		public PreviewDocument(IWindowManager windows)
		{
			this.windows = windows;
		}

		public void Execute(DocumentViewModel document)
		{
			windows.ShowDialog(document, "Preview");
		}
	}
}
using System.ComponentModel.Composition;
using System.Windows;

namespace Raven.Studio.Commands
{
	[Export]
	public class CopyDocumentIdToClipboard
	{
		public void Execute(string documentId)
		{
			Clipboard.SetText(documentId);
		}

		public bool CanExecute(string documentId)
		{
			return !string.IsNullOrEmpty(documentId);
		}
	}
}
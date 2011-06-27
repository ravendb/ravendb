using System.ComponentModel.Composition;
using System.Windows;
using Newtonsoft.Json;
using Raven.Studio.Features.Documents;

namespace Raven.Studio.Commands
{
	[Export]
	public class CopyDocumentToClipboard
	{
		public void Execute(DocumentViewModel document)
		{
			Clipboard.SetText(document.Contents.ToString(Formatting.Indented));
		}

		public bool CanExecute(DocumentViewModel document)
		{
			return document != null;
		}
	}
}
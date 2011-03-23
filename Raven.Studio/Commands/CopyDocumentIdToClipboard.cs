namespace Raven.Studio.Commands
{
    using System.Windows;

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
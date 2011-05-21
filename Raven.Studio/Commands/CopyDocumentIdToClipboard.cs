namespace Raven.Studio.Commands
{
    using System.Collections;
    using System.Linq;
    using System.Windows;
    using Features.Documents;

    public class CopyDocumentIdToClipboard
    {
        public bool CanExecute(object listOrId)
        {
            if (listOrId == null)
                return false;

            var list = listOrId as IList;
            if (list != null)
            {
                return list.Count > 0;
            }

            var viewModel = listOrId as string;
            return !string.IsNullOrWhiteSpace(viewModel);
        }

        public void Execute(object listOrId)
        {
            var list = listOrId as IList;
            if (list != null)
            {
                var document = list.OfType<DocumentViewModel>();
                var text = string.Join(", ", document.Select(x => x.Id));

                Clipboard.SetText(text);
            }
            else {
                Clipboard.SetText(listOrId.ToString());
            }
        }
    }
}
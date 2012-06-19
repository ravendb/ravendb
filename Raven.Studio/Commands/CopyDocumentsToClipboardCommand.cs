using System.Collections.Generic;
using System.Linq;
using System.Windows;
using Raven.Imports.Newtonsoft.Json;
using Raven.Studio.Features.Documents;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Commands
{
	public class CopyDocumentsToClipboardCommand : VirtualItemSelectionCommand<ViewableDocument>
	{
        public CopyDocumentsToClipboardCommand(ItemSelection<VirtualItem<ViewableDocument>> itemSelection)
            : base(itemSelection)
	    {
	    }

	    protected override bool CanExecuteOverride(IList<ViewableDocument> items)
        {
            return items.Count > 0;
        }

        protected override void ExecuteOverride(IList<ViewableDocument> realizedItems)
        {
            var newLine = System.Environment.NewLine;

            var documents = realizedItems
                .Select(x => string.IsNullOrEmpty(x.Id) ? "" : x.Id + newLine + x.Document.DataAsJson.ToString(Formatting.Indented))
                .ToList();

            
            Clipboard.SetText(documents.Count > 1 ? string.Join(newLine+newLine, documents) : documents.First());
        }
	}
}
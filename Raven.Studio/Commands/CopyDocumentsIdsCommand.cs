using System.Collections.Generic;
using System.Linq;
using System.Windows;
using Raven.Studio.Features.Documents;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Commands
{
	public class CopyDocumentsIdsCommand : VirtualItemSelectionCommand<ViewableDocument>
	{
	    public CopyDocumentsIdsCommand(ItemSelection<VirtualItem<ViewableDocument>> itemSelection) : base(itemSelection)
	    {
	    }

	    protected override bool CanExecuteOverride(IList<ViewableDocument> items)
        {
            return items.Count > 0;
        }

        protected override void ExecuteOverride(IList<ViewableDocument> realizedItems)
        {
            var documentsIds = realizedItems
                .Select(x => x.Id)
                .ToList();

            CopyIDs(documentsIds);
        }
		

		private void CopyIDs(IList<string> documentIds)
		{
			Clipboard.SetText(documentIds.Count > 1 ? string.Join(", ", documentIds) : documentIds.First());
		}
	}
}
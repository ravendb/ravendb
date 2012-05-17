using System.Collections.Generic;
using System.Linq;
using System.Windows;
using Raven.Studio.Features.Documents;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Commands
{
	public class CopyDocumentsIdsCommand : ListBoxCommand<VirtualItem<ViewableDocument>>
	{
		public override void Execute(object parameter)
		{
			var documentsIds = SelectedItems
                .Where(v => v.IsRealized)
				.Select(x => x.Item.Id)
				.ToList();

			CopyIDs(documentsIds);
		}

		private void CopyIDs(IList<string> documentIds)
		{
			Clipboard.SetText(documentIds.Count > 1 ? string.Join(", ", documentIds) : documentIds.First());
		}
	}
}
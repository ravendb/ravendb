using System.Collections.Generic;
using System.Linq;
using System.Windows;
using Raven.Studio.Features.Documents;

namespace Raven.Studio.Commands
{
	public class CopyDocumentsIdsCommand : ListBoxCommand<ViewableDocument>
	{
		public override void Execute(object parameter)
		{
			var documentsIds = Items
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
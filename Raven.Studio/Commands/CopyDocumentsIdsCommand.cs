using System.Collections.Generic;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using Raven.Studio.Features.Documents;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Commands
{
	public class CopyDocumentsIdsCommand : Command
	{
		private ListBox listBox;

		public override bool CanExecute(object parameter)
		{
			listBox = GetList(parameter);
			return listBox != null && listBox.SelectedItems.Count > 0;
		}

		public override void Execute(object parameter)
		{
			var documents = listBox.SelectedItems
				.Cast<ViewableDocument>()
				.ToList();
			var documentsIds = documents
				.Select(x => x.Id)
				.ToList();

			CopyIDs(documentsIds);
		}

		private void CopyIDs(IList<string> documentIds)
		{
			Clipboard.SetText(documentIds.Count > 1 ? string.Join(", ", documentIds) : documentIds.First());
		}

		private static ListBox GetList(object parameter)
		{
			if (parameter == null)
				return null;
			var menuItem = (MenuItem) parameter;
			var contextMenu = (ContextMenu) menuItem.Parent;
			if (contextMenu == null)
				return null;
			return (ListBox) contextMenu.Owner;
		}
	}
}
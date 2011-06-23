using System.Collections.Generic;
using Caliburn.Micro;
using Raven.Studio.Commands;
using Raven.Studio.Features.Documents;
using Raven.Studio.Features.Documents.Resources;
using SL4PopupMenu;

namespace Raven.Studio.Behaviors
{
	public class DocumentsContextMenu : ContextMenuBase<DocumentViewModel>
	{
		public DocumentsContextMenu(IList<DocumentViewModel> selectedItems)
			: base(selectedItems)
		{
		}

		protected override void GenerateMenuItems()
		{
			if (IsInMultiSelectedMode == false)
			{
				var editDocumentMenuItem = new PopupMenuItem(null, DocumentsResources.DocumentMenu_EditDocument);
				editDocumentMenuItem.Click += (s, ea) => IoC.Get<EditDocument>().Execute(SelectedItem);
				Menu.AddItem(editDocumentMenuItem);

				var copyIdMenuItem = new PopupMenuItem(null, DocumentsResources.DocumentMenu_CopyId);
				copyIdMenuItem.Click += (s, ea) => IoC.Get<CopyDocumentIdToClipboard>().Execute(SelectedItem.Id);
				Menu.AddItem(copyIdMenuItem);

				Menu.AddSeparator();

				Menu.AddItem(DocumentsResources.DocumentMenu_DeleteDocument, null);
			}
			else
			{
				Menu.AddItem(string.Format(DocumentsResources.DocumentMenu_DeleteDocuments, SelectedItems.Count), null);
			}
		}
	}
}
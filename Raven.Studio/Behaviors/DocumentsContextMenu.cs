using System.Collections.Generic;
using System.Linq;
using Caliburn.Micro;
using Raven.Studio.Commands;
using Raven.Studio.Features.Collections;
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
			if (IsInMultiSelectedMode)
			{
				if (SelectedItem.CollectionType != BuiltinCollectionName.Projection)
				{
					var deleteDocumentsMenuItem = new PopupMenuItem(null, string.Format(DocumentsResources.DocumentMenu_DeleteDocuments, SelectedItems.Count));
					deleteDocumentsMenuItem.Click += (s, ea) => IoC.Get<DeleteDocument>().Execute(SelectedItems.Select(item => item.Id).ToList());
					Menu.AddItem(deleteDocumentsMenuItem);
				}
			}
			else
			{
				var editDocumentMenuItem = new PopupMenuItem(null, DocumentsResources.DocumentMenu_EditDocument);
				editDocumentMenuItem.Click += (s, ea) => IoC.Get<EditDocument>().Execute(SelectedItem);
				Menu.AddItem(editDocumentMenuItem);

				if (SelectedItem.CollectionType == BuiltinCollectionName.Projection)
				{
					var copyProjectionMenuItem = new PopupMenuItem(null, DocumentsResources.DocumentMenu_CopyProjection);
					copyProjectionMenuItem.Click += (s, ea) => IoC.Get<CopyDocumentToClipboard>().Execute(SelectedItem);
					Menu.AddItem(copyProjectionMenuItem);
				}
				else
				{
					var copyIdMenuItem = new PopupMenuItem(null, DocumentsResources.DocumentMenu_CopyId);
					copyIdMenuItem.Click += (s, ea) => IoC.Get<CopyDocumentIdToClipboard>().Execute(SelectedItem.Id);
					Menu.AddItem(copyIdMenuItem);

					Menu.AddSeparator();

					var deleteDocumentMenuItem = new PopupMenuItem(null, DocumentsResources.DocumentMenu_DeleteDocument);
					deleteDocumentMenuItem.Click +=
						(s, ea) => IoC.Get<DeleteDocument>().Execute(SelectedItems.Select(item => item.Id).ToList());
					Menu.AddItem(deleteDocumentMenuItem);
				}
			}
		}
	}
}
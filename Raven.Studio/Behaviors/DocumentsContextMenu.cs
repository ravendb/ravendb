using System;
using System.Collections.Generic;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using System.Windows.Media;
using Caliburn.Micro;
using Raven.Studio.Commands;
using Raven.Studio.Common;
using Raven.Studio.Features.Documents;
using Raven.Studio.Features.Documents.Resources;
using SL4PopupMenu;

namespace Raven.Studio.Behaviors
{
	public class DocumentsContextMenu
	{
		private readonly IList<DocumentViewModel> selectedItems;
		private readonly DocumentViewModel selectedItem;
		private readonly bool isInMultiSelectedMode;

		private PopupMenu menu;
		private PopupMenuItem editDocumentMenuItem;
		private PopupMenuItem copyIdMenuItem;

		public DocumentsContextMenu(IList<DocumentViewModel> selectedItems)
		{
			if (selectedItems == null)
				throw new ArgumentNullException("selectedItems", "Selected items cannot be null");

			this.selectedItems = selectedItems;
			if (selectedItems.Count > 1)
				isInMultiSelectedMode = true;
			else
			{
				selectedItem = selectedItems.FirstOrDefault();
				if (selectedItem == null)
					throw new InvalidOperationException("Selected items must contain at least one item");
			}
		}

		public void Open(FrameworkElement element, MouseButtonEventArgs e)
		{
			CreateMenu();
			menu.Open(Mouse.Position, MenuOrientationTypes.MouseBottomRight, 0, element, true, e);
		}

		private void FocusTheClickOnItem(object sender, RoutedEventArgs e)
		{
			var elementsInHostCoordinates = VisualTreeHelper.FindElementsInHostCoordinates(Mouse.Position,
			                                                                               Application.Current.RootVisual);
			elementsInHostCoordinates
				.Where(element => element is ListBoxItem)
				.OfType<ListBoxItem>()
				.ToList()
				.ForEach(item =>
				         	{
				         		var parent = VisualTreeHelperExtensions.GetParentOfType<ListBox>(item);
				         		if (parent != null && parent.SelectionMode != SelectionMode.Single)
				         			parent.SelectedItems.Clear();
				         		item.IsSelected = true;
				         	});
		}

		private void CreateMenu()
		{
			menu = new PopupMenu();

			if (isInMultiSelectedMode == false)
			{
				editDocumentMenuItem = new PopupMenuItem(null, DocumentsResources.DocumentMenu_EditDocument);
				editDocumentMenuItem.Click += (s, ea) =>
				                              	{
				                              		IoC.Get<EditDocument>().Execute(selectedItem);
				                              	};
				menu.AddItem(editDocumentMenuItem);

				copyIdMenuItem = new PopupMenuItem(null, DocumentsResources.DocumentMenu_CopyId);
				copyIdMenuItem.Click += (s, ea) => IoC.Get<CopyDocumentIdToClipboard>().Execute(selectedItem.Id);
				menu.AddItem(copyIdMenuItem);

				menu.AddSeparator();
			}

			menu.AddItem(DocumentsResources.DocumentMenu_DeleteDocument, null);

			var canvas = menu.ItemsControl.Parent as Canvas;
			if (canvas != null) canvas.MouseMove += (s, e) => { Mouse.Position = e.GetPosition(null); };

			menu.Closing += FocusTheClickOnItem;
		}
	}
}
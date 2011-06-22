using System;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Interactivity;
using System.Windows.Media;
using Caliburn.Micro;
using Raven.Studio.Common;
using Raven.Studio.Features.Documents;
using Raven.Studio.Features.Documents.Resources;
using SL4PopupMenu;

namespace Raven.Studio.Behaviors
{
	public class AttachDocumentsMenu : Behavior<ListBox>
	{
		private PopupMenu menu;
		private Point mousePosition;

		protected override void OnAttached()
		{
			CreateMenu();

			base.OnAttached();
		}

		private void CreateMenu()
		{
			menu = new PopupMenu();
			
			menu.AddItem(new PopupMenuItem(null, DocumentsResources.DocumentMenu_EditDocument));
			menu.AddItem(DocumentsResources.DocumentMenu_CopyId, null);
			menu.AddSeparator();
			menu.AddItem(DocumentsResources.DocumentMenu_DeleteDocument, null);
			
			var canvas = menu.ItemsControl.Parent as Canvas;
			if (canvas != null) canvas.MouseMove += (s, e) => { mousePosition = e.GetPosition(null); };

			menu.Opening += OnMenuOpening;
			menu.Closing += FocusTheClickOnItem;
			menu.AddTrigger(TriggerTypes.RightClick, AssociatedObject);
		}

		private void FocusTheClickOnItem(object sender, RoutedEventArgs e)
		{

			var elementsInHostCoordinates = VisualTreeHelper.FindElementsInHostCoordinates(mousePosition,
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

		private void OnMenuOpening(object sender, RoutedEventArgs e)
		{
			OpenOnlyOnDocumentItem(e.OriginalSource);
			if (menu.IsOpeningCancelled)
				return;

			var editDocument = (PopupMenuItem)menu.Items[0];
			editDocument.SetValue(Caliburn.Micro.Action.TargetWithoutContextProperty, "EditDocument");
			editDocument.SetValue(Message.AttachProperty, null);
			editDocument.SetValue(Message.AttachProperty, "Execute($dataContext)");
			editDocument.DataContext = AssociatedObject.SelectedItem;

			MultiItemsMenuOrSingleItemMenu();
		}

		private void MultiItemsMenuOrSingleItemMenu()
		{
			var editDocument = (PopupMenuItem)menu.Items[0];

			if (AssociatedObject.SelectionMode != SelectionMode.Single &&
				AssociatedObject.SelectedItems.Count > 1)
			{
				editDocument.Visibility = Visibility.Collapsed;
			}
			else
			{
				editDocument.Visibility = Visibility.Visible;
			}
		}

		// Make sure that the menu opened only on a document item, 
		// and not on an empty space.
		private void OpenOnlyOnDocumentItem(object element)
		{
			var ele = element as DependencyObject;
			menu.IsOpeningCancelled = true;
			while (ele != null && (ele is ScrollViewer) == false)
			{
				var item = ele as ListBoxItem;
				if (item != null)
				{
					menu.IsOpeningCancelled = false;

					// Make the current element selected on right click
					if (AssociatedObject.SelectionMode != SelectionMode.Single && AssociatedObject.SelectedItems.Contains(item.DataContext) == false)
						AssociatedObject.SelectedItems.Clear();
					item.IsSelected = true;

					break;
				}
				ele = VisualTreeHelper.GetParent(ele);
			}
		}

		protected override void OnDetaching()
		{
			base.OnDetaching();
		}
	}
}
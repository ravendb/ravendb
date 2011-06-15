using System;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Interactivity;
using System.Windows.Media;
using Raven.Studio.Common;
using SL4PopupMenu;

namespace Raven.Studio.Behaviors
{
	public class AttachDocumentsMenu : Behavior<ListBox>
	{
		private PopupMenu menu;

		protected override void OnAttached()
		{
			CreateMenu();

			//TargetMenu.OpenNextTo(MenuOrientationTypes.MouseBottomRight, null, true, true);

			base.OnAttached();
		}

		public static Point MousePosition { get; set; }
		
		private void CreateMenu()
		{
			menu = new PopupMenu();
			menu.AddItem("Edit Document", null);
			menu.AddItem("Copy Document Id to Clipboard", null);
			menu.AddSeparator();
			menu.AddItem("Delete Document", null);

			var canvas = menu.ItemsControl.Parent as Canvas;
			if (canvas != null) canvas.MouseMove += (s, e) => { MousePosition = e.GetPosition(null); };

			menu.Opening += OpenOnlyOnDocumentItem;
			menu.Closing += FocusTheClickOnItem;
			menu.AddTrigger(TriggerTypes.RightClick, AssociatedObject);

			//        <popupMenu:PopupMenu x:Name="menu">
			//    <ListBox>
			//        <popupMenu:PopupMenuItem Header="Edit Document"
			//                                 cm:Action.TargetWithoutContext="EditDocument"
			//                                 cm:Message.Attach="[Click]=[EditDocument($selectedItems)]" />
			//        <popupMenu:PopupMenuItem Header="Copy Document Id to Clipboard" />
			//        <popupMenu:PopupMenuSeparator />
			//        <popupMenu:PopupMenuItem Header="Delete Document" />
			//    </ListBox>
			//</popupMenu:PopupMenu>
		}

		private void FocusTheClickOnItem(object sender, RoutedEventArgs e)
		{

			var elementsInHostCoordinates = VisualTreeHelper.FindElementsInHostCoordinates(MousePosition,
																						   Application.Current.RootVisual);
			elementsInHostCoordinates
				.Where(element => element is ListBoxItem)
				.OfType<ListBoxItem>()
				.ToList()
				.ForEach(FocusClickOnItem);
		}

		private void OpenOnlyOnDocumentItem(object sender, RoutedEventArgs e)
		{
			// Make sure that the menu opened only on a document item, 
			// and not on an empty space.
			var ele = e.OriginalSource as DependencyObject;		// ListBoxItem | ContentControl | Control | FrameworkElement
			menu.IsOpeningCancelled = true;
			while (ele != null && (ele is ScrollViewer) == false)
			{
				var item  = ele as ListBoxItem;
				if (item != null)
				{
					menu.IsOpeningCancelled = false;
					FocusClickOnItem(item, AssociatedObject);		// Make the current element selected on right click
					break;
				}
				ele = VisualTreeHelper.GetParent(ele);
			}
		}

		private static void FocusClickOnItem(ListBoxItem item, ListBox parent)
		{
			if (parent.SelectionMode != SelectionMode.Single && parent.SelectedItems.Contains(item.DataContext) == false)
				parent.SelectedItems.Clear();
			item.IsSelected = true;
		}

		private static void FocusClickOnItem(ListBoxItem item)
		{
			var parent = VisualTreeHelperExtensions.GetParentOfType<ListBox>(item);
			if (parent == null)
				throw new InvalidOperationException("ListBoxItem must have a ancestor of type listbox");
			FocusClickOnItem(item, parent);
		}

		protected override void OnDetaching()
		{
			base.OnDetaching();
		}
	}
}
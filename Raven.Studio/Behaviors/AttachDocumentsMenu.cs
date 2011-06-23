using System.Collections.Generic;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using System.Windows.Interactivity;
using System.Windows.Media;
using Raven.Studio.Features.Documents;

namespace Raven.Studio.Behaviors
{
	public class AttachDocumentsMenu : Behavior<ListBox>
	{
		protected override void OnAttached()
		{
			AssociatedObject.MouseRightButtonDown += AssociatedObject_MouseRightButtonDown;
			AssociatedObject.MouseRightButtonUp += AssociatedObject_MouseRightButtonUp;
		}

		protected override void OnDetaching()
		{
			AssociatedObject.MouseRightButtonDown -= AssociatedObject_MouseRightButtonDown;
			AssociatedObject.MouseRightButtonUp -= AssociatedObject_MouseRightButtonUp;
		}

		private void AssociatedObject_MouseRightButtonDown(object sender, MouseButtonEventArgs e)
		{
			// This is required for the MouseRightButtonUp event to be fired. 
			e.Handled = true;
		}

		private void AssociatedObject_MouseRightButtonUp(object sender, MouseButtonEventArgs e)
		{
			var item = VisualTreeHelper.FindElementsInHostCoordinates(e.GetPosition(null), AssociatedObject)
				.OfType<ListBoxItem>()
				.FirstOrDefault();

			if (item == null)
				return;

			// Make the current element selected on right click
			if (AssociatedObject.SelectionMode != SelectionMode.Single && AssociatedObject.SelectedItems.Contains(item.DataContext) == false)
				AssociatedObject.SelectedItems.Clear();
			item.IsSelected = true;

			var menu = new DocumentsContextMenu(AssociatedObject.SelectedItems.Cast<DocumentViewModel>().ToList());
			menu.Open(item, e);
		}
	}
}
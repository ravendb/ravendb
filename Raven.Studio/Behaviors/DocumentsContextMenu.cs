using System.Linq;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using System.Windows.Interactivity;
using System.Windows.Media;

namespace Raven.Studio.Behaviors
{
	public class DocumentsContextMenu : Behavior<ListBox>
	{
		private ContextMenu menu;

		protected override void OnAttached()
		{
			menu = new ContextMenu {Visibility = Visibility.Collapsed};
			menu.Closed += (o, ea) =>
							{
								menu.Visibility = Visibility.Collapsed;
							};
			AssociatedObject.SetValue(ContextMenuService.ContextMenuProperty, menu);

			AssociatedObject.MouseRightButtonDown += AssociatedObject_MouseRightButtonDown;
			AssociatedObject.MouseRightButtonUp += AssociatedObject_MouseRightButtonUp;

			base.OnAttached();
		}

		protected override void OnDetaching()
		{
			AssociatedObject.MouseRightButtonDown -= AssociatedObject_MouseRightButtonDown;
			AssociatedObject.MouseRightButtonUp -= AssociatedObject_MouseRightButtonUp;

			menu = null;
			AssociatedObject.SetValue(ContextMenuService.ContextMenuProperty, menu);

			base.OnDetaching();
		}

		private void AssociatedObject_MouseRightButtonUp(object sender, MouseButtonEventArgs e)
		{
			var item = VisualTreeHelper.FindElementsInHostCoordinates(e.GetPosition(null), AssociatedObject)
				.OfType<ListBoxItem>()
				.FirstOrDefault();

			if (item == null)
				return;

			menu.Visibility = Visibility.Visible;
		}

		private void AssociatedObject_MouseRightButtonDown(object sender, MouseButtonEventArgs e)
		{
			// This is required for the MouseRightButtonUp event to be fired. 
			e.Handled = true;
		}
	}
}
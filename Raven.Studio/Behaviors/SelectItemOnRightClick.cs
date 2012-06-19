// -----------------------------------------------------------------------
//  <copyright file="SelectItemOnRightClick.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using System.Windows.Controls;
using System.Windows.Input;
using System.Windows.Interactivity;
using System.Windows.Media;

namespace Raven.Studio.Behaviors
{
	public class SelectItemOnRightClick : Behavior<ListBox>
	{
		protected override void OnAttached()
		{
			base.OnAttached();
			AssociatedObject.MouseRightButtonDown += AssociatedObjectOnMouseRightButtonDown;
		}

		protected override void OnDetaching()
		{
			base.OnDetaching();

			AssociatedObject.MouseRightButtonDown -= AssociatedObjectOnMouseRightButtonDown;
		}

		void AssociatedObjectOnMouseRightButtonDown(object sender, MouseButtonEventArgs e)
		{
			var item = VisualTreeHelper.FindElementsInHostCoordinates(e.GetPosition(null),AssociatedObject)
				.OfType<ListBoxItem>()
				.FirstOrDefault();

			if (item == null) 
				return;

			if (AssociatedObject.SelectionMode != SelectionMode.Single && AssociatedObject.SelectedItems.Contains(item.DataContext) == false)
				AssociatedObject.SelectedItems.Clear();

			item.IsSelected = true;
		}
	}
}
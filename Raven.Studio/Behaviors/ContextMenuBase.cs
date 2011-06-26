using System;
using System.Collections.Generic;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using System.Windows.Media;
using Caliburn.Micro;
using Raven.Studio.Common;
using SL4PopupMenu;

namespace Raven.Studio.Behaviors
{
	public abstract class ContextMenuBase<T> where T : class
	{
		protected readonly IList<T> SelectedItems;
		protected readonly T SelectedItem;
		protected readonly bool IsInMultiSelectedMode;
		protected readonly PopupMenu Menu;

		protected ContextMenuBase(IList<T> selectedItems)
		{
			if (selectedItems == null)
				throw new ArgumentNullException("selectedItems", "Selected items cannot be null");

			SelectedItems = selectedItems;
			if (SelectedItems.Count > 1)
				IsInMultiSelectedMode = true;
			
			SelectedItem = SelectedItems.FirstOrDefault();
			if (SelectedItem == null)
				throw new InvalidOperationException("Selected items must contain at least one item");

			Menu = new PopupMenu();

			var canvas = Menu.ItemsControl.Parent as Canvas;
			if (canvas != null) canvas.MouseMove += (s, e) => { Mouse.Position = e.GetPosition(null); };

			Menu.Closing += OnMneuClosing;
		}

		protected abstract void GenerateMenuItems();

		public void Open(FrameworkElement element, MouseButtonEventArgs e)
		{
			GenerateMenuItems();
			if (Menu.Items.Count == 0)
				return;
			Menu.Open(Mouse.Position, MenuOrientationTypes.MouseBottomRight, 0, element, true, e);
		}

		private void OnMneuClosing(object sender, RoutedEventArgs e)
		{
			FocusTheClickOnItem();
		}
		private void FocusTheClickOnItem()
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
	}
}
using System.Collections.Generic;
using System.Linq;
using System.Windows.Controls;
using System.Windows.Interactivity;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Commands
{
	public abstract class ListBoxCommand<T> : Command
	{
		private ListBox listBox;
		private ListBox ListBox
		{
			get { return listBox; }
			set
			{
				listBox = value;
				items = null;
			}
		}

		private List<T> items;
		protected List<T> Items
		{
			get { return items ?? (items = ListBox.SelectedItems.Cast<T>().ToList()); }
		}

		protected object Context
		{
			get { return ListBox.DataContext; }
		}

		public override bool CanExecute(object parameter)
		{
			items = null;
			ListBox = GetList(parameter);
			return ListBox != null && ListBox.SelectedItems.Count > 0;
		}

		private static ListBox GetList(object parameter)
		{
			if (parameter == null)
				return null;
			var attachedObject = parameter as IAttachedObject;
			if (attachedObject != null)
				return (ListBox)attachedObject.AssociatedObject;

			var menuItem = (MenuItem)parameter;
			var contextMenu = (ContextMenu)menuItem.Parent;
			if (contextMenu == null)
				return null;
			return (ListBox)contextMenu.Owner;
		}
	}
}
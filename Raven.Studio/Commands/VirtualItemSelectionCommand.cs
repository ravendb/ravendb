using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Commands
{
    public abstract class VirtualItemSelectionCommand<T> : Command where T:class 
    {
        private readonly ItemSelection<VirtualItem<T>> itemSelection;
        private IList<VirtualItem<T>> currentItems;
 
        public VirtualItemSelectionCommand(ItemSelection<VirtualItem<T>> itemSelection)
        {
            this.itemSelection = itemSelection;
            itemSelection.SelectionChanged += HandleSelectionChanged;
            CacheCurrentItems();
        }

        protected ItemSelection<VirtualItem<T>> ItemSelection
        {
            get { return itemSelection; }
        }

        private void HandleSelectionChanged(object sender, EventArgs e)
        {
            if (currentItems != null)
            {
                foreach (var item in currentItems)
                {
                    item.PropertyChanged -= HandleVirtualItemChanged;
                }
            }

            CacheCurrentItems();

            RaiseCanExecuteChanged();
        }

        private void CacheCurrentItems()
        {
            currentItems = ItemSelection.GetSelectedItems().ToList();

            foreach (var item in currentItems)
            {
                item.PropertyChanged += HandleVirtualItemChanged;
            }
        }

        private void HandleVirtualItemChanged(object sender, PropertyChangedEventArgs e)
        {
            RaiseCanExecuteChanged();
        }

        public override sealed bool CanExecute(object parameter)
        {
            return CanExecuteOverride(GetRealizedItems());
        }

        public override sealed void Execute(object parameter)
        {
            ExecuteOverride(GetRealizedItems());
        }

        private List<T> GetRealizedItems()
        {
            return currentItems
                .EmptyIfNull()
                .Where(v => v.IsRealized)
                .Select(v => v.Item)
                .ToList();
        }

        protected void ClearSelection()
        {
            ItemSelection.SetDesiredSelection(new VirtualItem<T>[0]);
        }

        protected virtual bool CanExecuteOverride(IList<T> items)
        {
            return true;
        }

        protected abstract void ExecuteOverride(IList<T> realizedItems);
    }
}

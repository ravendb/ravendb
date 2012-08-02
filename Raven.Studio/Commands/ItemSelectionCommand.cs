using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Commands
{
    public abstract class ItemSelectionCommand<T> : Command where T:class 
    {
        private readonly ItemSelection<T> itemSelection;

        public ItemSelectionCommand(ItemSelection<T> itemSelection)
        {
            this.itemSelection = itemSelection;
            itemSelection.SelectionChanged += HandleSelectionChanged;
        }

        protected ItemSelection<T> ItemSelection
        {
            get { return itemSelection; }
        }

        private void HandleSelectionChanged(object sender, EventArgs e)
        {
            RaiseCanExecuteChanged();
        }

        public override sealed bool CanExecute(object parameter)
        {
            return CanExecuteOverride(GetItems());
        }

        public override sealed void Execute(object parameter)
        {
            ExecuteOverride(GetItems());
        }

        private IEnumerable<T> GetItems()
        {
            return ItemSelection.GetSelectedItems()
                .EmptyIfNull();
        }

        protected void ClearSelection()
        {
            ItemSelection.SetDesiredSelection(new T[0]);
        }

        protected virtual bool CanExecuteOverride(IEnumerable<T> items)
        {
            return true;
        }

        protected abstract void ExecuteOverride(IEnumerable<T> items);
    }
}

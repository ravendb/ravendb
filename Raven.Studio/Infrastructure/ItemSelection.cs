using System;
using System.Collections;

namespace Raven.Studio.Infrastructure
{
    public class ItemSelection
    {
        private int count;
        private Func<IEnumerable> selectedItemsProvider;

        public event EventHandler<EventArgs> SelectionChanged;

        public event EventHandler<DesiredSelectionChangedEventArgs> DesiredSelectionChanged;

        public void NotifySelectionChanged(int count, Func<IEnumerable> selectedItemsEnumerableProvider)
        {
            this.count = count;
            this.selectedItemsProvider = selectedItemsEnumerableProvider;
            OnSelectionChanged(EventArgs.Empty);
        }

        int Count { get { return count; } }

        protected IEnumerable GetSelectedItems()
        {
            if (selectedItemsProvider != null)
            {
                return selectedItemsProvider();
            }
            else
            {
                return new object[0];
            }
        }

        protected void SetDesiredSelection(IEnumerable items)
        {
            OnDesiredSelectionChanged(new DesiredSelectionChangedEventArgs(items));
        }

        protected virtual void OnDesiredSelectionChanged(DesiredSelectionChangedEventArgs e)
        {
            var handler = DesiredSelectionChanged;
            if (handler != null) handler(this, e);
        }

        protected virtual void OnSelectionChanged(EventArgs e)
        {
            var handler = SelectionChanged;
            if (handler != null) handler(this, e);
        }

        public void ClearSelection()
        {
            SetDesiredSelection(new object[0]);
        }
    }

    public class DesiredSelectionChangedEventArgs : EventArgs
    {
        private readonly IEnumerable items;

        public DesiredSelectionChangedEventArgs(IEnumerable items)
        {
            this.items = items;
        }

        public IEnumerable Items
        {
            get { return items; }
        }
    }
}

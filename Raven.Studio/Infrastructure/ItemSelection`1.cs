using System;
using System.Collections.Generic;
using System.Linq;

namespace Raven.Studio.Infrastructure
{
    public class ItemSelection<T> : ItemSelection
    {
        public new IEnumerable<T> GetSelectedItems()
        {
            return base.GetSelectedItems().OfType<T>();
        }

        public void SetDesiredSelection(IEnumerable<T> items)
        {
            base.SetDesiredSelection(items);
        }
    }
}

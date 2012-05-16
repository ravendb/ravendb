using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Net;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;

namespace Raven.Studio.Features.Documents
{
    public class SuggestedColumnCollection : KeyedCollection<string, SuggestedColumn>
    {
        public SuggestedColumnCollection()
        {
            
        }

        public SuggestedColumnCollection(IEnumerable<SuggestedColumn> items)
        {
            foreach (var suggestedColumn in items)
            {
                Add(suggestedColumn);
            }
        }

        protected override string GetKeyForItem(SuggestedColumn item)
        {
            return item.Binding;
        }
    }
}

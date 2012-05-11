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
using System.Linq;

namespace Raven.Studio.Features.Documents
{
    public class SuggestedColumn
    {
        public string Header { get; set; }

        public string Binding { get; set; }

        public IList<SuggestedColumn> Children { get; set; }

        public ColumnDefinition ToColumnDefinition()
        {
            return new ColumnDefinition() {Header = Header, Binding = Binding};
        }

        public void MergeFrom(SuggestedColumn suggestedColumn)
        {
            if (suggestedColumn.Binding != Binding)
            {
                return;
            }

            foreach (var otherChild in suggestedColumn.Children)
            {
                var similarChild = Children.FirstOrDefault(c => c.Binding == otherChild.Binding);
                if (similarChild == null)
                {
                    Children.Add(otherChild);
                }
                else
                {
                    similarChild.MergeFrom(otherChild);
                }
            }
        }
    }
}

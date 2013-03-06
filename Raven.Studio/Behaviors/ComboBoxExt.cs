using System;
using System.Net;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;

namespace Raven.Studio.Behaviors
{
    public class ComboBoxExt
    {
        public static DataTemplate GetSelectionBoxItemTemplate(DependencyObject obj)
        {
            return (DataTemplate) obj.GetValue(SelectionBoxItemTemplateProperty);
        }

        public static void SetSelectionBoxItemTemplate(DependencyObject obj, DataTemplate value)
        {
            obj.SetValue(SelectionBoxItemTemplateProperty, value);
        }

        public static readonly DependencyProperty SelectionBoxItemTemplateProperty =
            DependencyProperty.RegisterAttached("SelectionBoxItemTemplate", typeof (DataTemplate), typeof (ComboBoxExt),
                                                new PropertyMetadata(null));

    }
}

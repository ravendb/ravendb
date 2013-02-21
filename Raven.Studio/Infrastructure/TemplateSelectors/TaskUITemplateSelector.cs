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
using Raven.Studio.Models;

namespace Raven.Studio.Infrastructure.TemplateSelectors 
{
    public class TaskUITemplateSelector : DataTemplateSelector
    {
        public DataTemplate Input { get; set; }
        public DataTemplate CheckBox { get; set; }

        public override DataTemplate SelectTemplate(object item, DependencyObject container)
        {
            if (item as TaskInput != null) return Input;
            if (item as TaskCheckBox != null) return CheckBox;

            return base.SelectTemplate(item, container);
        }
    }
}

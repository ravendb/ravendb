using System.Windows;
using Raven.Studio.Features.Tasks;

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

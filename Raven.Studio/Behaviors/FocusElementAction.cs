using System.Windows.Controls;
using System.Windows.Interactivity;

namespace Raven.Studio.Behaviors
{
    public class FocusElementAction : TargetedTriggerAction<Control>
    {
        protected override void Invoke(object parameter)
        {
            if (Target != null)
            {
                AssociatedObject.Dispatcher.BeginInvoke(() => Target.Focus());
            }
        }
    }
}

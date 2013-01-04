using System.Windows.Controls;
using System.Windows.Interactivity;

namespace Raven.Studio.Behaviors
{
    public class SelectAllAction : TargetedTriggerAction<TextBox>
    {
        protected override void Invoke(object parameter)
        {
            Target.SelectAll();
        }
    }
}

using System.Windows.Controls;
using System.Windows.Interactivity;

namespace Raven.Studio.Behaviors
{
    public class SelectAllAction : TriggerAction<TextBox>
    {
        protected override void Invoke(object parameter)
        {
            AssociatedObject.SelectAll();
        }
    }
}

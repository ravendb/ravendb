using System.Windows.Controls;
using System.Windows.Interactivity;

namespace Raven.Studio.Behaviors
{
    public class OpenDropDownAction : TriggerAction<AutoCompleteBox>
    {
        protected override void Invoke(object parameter)
        {
            AssociatedObject.IsDropDownOpen = true;
        }
    }
}
using System.Windows.Controls;
using System.Windows.Interactivity;

namespace Raven.Studio.Behaviors
{
    public class ShowAutoCompleteDropDown : TriggerAction<AutoCompleteBox>
    {
        protected override void Invoke(object parameter)
        {
            AssociatedObject.IsDropDownOpen = true;
        }
    }
}

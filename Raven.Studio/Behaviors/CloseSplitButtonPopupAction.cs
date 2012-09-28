using System.Windows.Interactivity;
using Delay;

namespace Raven.Studio.Behaviors
{
    public class CloseSplitButtonPopupAction : TargetedTriggerAction<SplitButtonPopup>
    {
        protected override void Invoke(object parameter)
        {
            Target.IsPopupOpen = false;
        }
    }
}
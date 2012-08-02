using System;
using System.Net;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Interactivity;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;
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

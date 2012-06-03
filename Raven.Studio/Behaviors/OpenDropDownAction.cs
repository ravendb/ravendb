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

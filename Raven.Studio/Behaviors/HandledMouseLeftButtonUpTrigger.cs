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
    public class HandledMouseLeftButtonUpTrigger : TriggerBase<UIElement>
    {
        protected override void OnAttached()
        {
            base.OnAttached();

            AssociatedObject.AddHandler(UIElement.MouseLeftButtonUpEvent, new MouseButtonEventHandler(HandleEvent), true);
        }

        protected override void OnDetaching()
        {
            AssociatedObject.RemoveHandler(UIElement.MouseLeftButtonUpEvent, new MouseButtonEventHandler(HandleEvent));
        }

        private void HandleEvent(object sender, MouseButtonEventArgs e)
        {
            InvokeActions(e);
        }
    }
}

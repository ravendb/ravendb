using System.Windows;
using System.Windows.Input;
using System.Windows.Interactivity;

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
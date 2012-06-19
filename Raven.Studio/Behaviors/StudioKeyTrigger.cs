using System.Windows;
using System.Windows.Input;
using System.Windows.Interactivity;
using Microsoft.Expression.Interactivity.Input;

namespace Raven.Studio.Behaviors
{
    /// <summary>
    /// The built in KeyTrigger is broken: it doesn't work for ChildWindows, and it 
    /// doesn't detach itself when the element gets unloaded. Which is why we've got our own.
    /// </summary>
    public class StudioKeyTrigger : EventTriggerBase<FrameworkElement>
    {
        public static readonly DependencyProperty KeyProperty =
            DependencyProperty.Register("Key", typeof(Key), typeof(StudioKeyTrigger), new PropertyMetadata(default(Key)));

        public static readonly DependencyProperty ModifiersProperty =
            DependencyProperty.Register("Modifiers", typeof(ModifierKeys), typeof(StudioKeyTrigger), new PropertyMetadata(default(ModifierKeys)));

        public ModifierKeys Modifiers
        {
            get { return (ModifierKeys)GetValue(ModifiersProperty); }
            set { SetValue(ModifiersProperty, value); }
        }

        public Key Key
        {
            get { return (Key)GetValue(KeyProperty); }
            set { SetValue(KeyProperty, value); }
        }

        protected override string GetEventName()
        {
            return "KeyDown";
        }

        protected override void OnEvent(System.EventArgs eventArgs)
        {
            var keyEventArgs = eventArgs as KeyEventArgs;
            if (keyEventArgs.Key == Key && Keyboard.Modifiers == Modifiers)
            {
                InvokeActions(null);
            }
        }
    }
}
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
using ActiproSoftware.Compatibility;
using ActiproSoftware.Windows.Controls.SyntaxEditor;

namespace Raven.Studio.Behaviors
{
    public class AddKeyBindingBehaviour : Behavior<SyntaxEditor>
    {
        private bool isAttached;

        public Key Key
        {
            get { return (Key)GetValue(KeyProperty); }
            set { SetValue(KeyProperty, value); }
        }

        public static readonly DependencyProperty KeyProperty =
            DependencyProperty.Register("Key", typeof(Key), typeof(AddKeyBindingBehaviour), new PropertyMetadata(Key.None, HandleChanged));



        public ModifierKeys ModifierKeys
        {
            get { return (ModifierKeys)GetValue(ModifierKeysProperty); }
            set { SetValue(ModifierKeysProperty, value); }
        }

        public static readonly DependencyProperty ModifierKeysProperty =
            DependencyProperty.Register("ModifierKeys", typeof(ModifierKeys), typeof(AddKeyBindingBehaviour), new PropertyMetadata(ModifierKeys.None, HandleChanged));

        

        public ICommand Command
        {
            get { return (ICommand)GetValue(CommandProperty); }
            set { SetValue(CommandProperty, value); }
        }

        public static readonly DependencyProperty CommandProperty =
            DependencyProperty.Register("Command", typeof(ICommand), typeof(AddKeyBindingBehaviour), new PropertyMetadata(null, HandleChanged));

        private KeyBinding binding;

        private static void HandleChanged(DependencyObject d, DependencyPropertyChangedEventArgs e)
        {
            (d as AddKeyBindingBehaviour).HandleChanged();
        }

        protected override void OnAttached()
        {
            base.OnAttached();
            isAttached = true;
            HandleChanged();
        }

        private void HandleChanged()
        {
            if (!isAttached)
            {
                return;
            }

            if (binding != null)
            {
                AssociatedObject.InputBindings.Remove(binding);
            }

            if (Key != Key.None && Command != null)
            {
                binding = new KeyBinding((ICommand)Command, Key, ModifierKeys);
                AssociatedObject.InputBindings.Add(binding);
            }
        }
    }
}

namespace Raven.Studio.Framework
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel.Composition;
    using System.Linq;
    using System.Windows;
    using System.Windows.Input;
    using Caliburn.Micro;
    using Action = System.Action;

    [Export(typeof (IKeyboardShortcutBinder))]
    public class KeyboardShortcutBinder : IKeyboardShortcutBinder
    {
        private readonly List<KeyCombination> bindings = new List<KeyCombination>();

        void Register(KeyCombination shortcut)
        {
            if (shortcut.ModifierKeys == ModifierKeys.Alt)
            {
                throw new NotSupportedException(
                    "Silverlight doesn't catch Alt! If you really, really want to do this. Look at the source here.");
                //NOTE: http://forums.silverlight.net/forums/p/85402/342319.aspx
            }
            bindings.Add(shortcut);
        }

        public void Initialize(FrameworkElement observable)
        {
            observable.KeyUp += HandleKeyUp;
            observable.Unloaded += delegate
                                       {
                                           var context = observable.DataContext;

                                           var remove = from binding in bindings
                                                        where binding.Context == context
                                                        select binding;

                                           remove.ToList().ForEach(x => bindings.Remove(x));

                                           observable.KeyUp -= HandleKeyUp;
                                       };
        }

        void HandleKeyUp(object sender, KeyEventArgs e)
        {
            if (bindings.Any(binding => binding.Matches(e.Key)))
                                        {
                                            e.Handled = true;
                                        }
        }

        public void Register(Key key, ModifierKeys modifierKeys, Action whenPressed, IScreen context)
        {
            Register(new KeyCombination(key, modifierKeys, whenPressed, context));
        }

        public void Register<T>(Key key, ModifierKeys modifierKeys, Action<T> whenPressed, IScreen context)
        {
            var command = IoC.Get<T>(typeof (T).Name);
            Register(new KeyCombination(key, modifierKeys, () => whenPressed(command), context));
        }
    }
}
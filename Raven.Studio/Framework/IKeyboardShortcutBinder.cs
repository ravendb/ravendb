namespace Raven.Studio.Framework
{
    using System;
    using System.Windows;
    using System.Windows.Input;
    using Caliburn.Micro;
    using Action = System.Action;

    public interface IKeyboardShortcutBinder
    {
        void Initialize(FrameworkElement observable);
        void Register(Key key, ModifierKeys modifierKeys, Action whenPressed, IScreen context);
        void Register<T>(Key key, ModifierKeys modifierKeys, Action<T> whenPressed, IScreen context);
    }
}
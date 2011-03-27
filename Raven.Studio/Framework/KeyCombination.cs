namespace Raven.Studio.Framework
{
    using System;
    using System.Windows.Input;
    using Caliburn.Micro;
    using Action = System.Action;

    public class KeyCombination
    {
        public KeyCombination(Key key, ModifierKeys? modifierKeys, Action whenPressed, IScreen context)
        {
            if (whenPressed == null)
                throw new ArgumentNullException("whenPressed",
                                                "You must provide an action to be executed when the key combination is pressed.");
            Key = key;
            ModifierKeys = modifierKeys;
            WhenPressed = whenPressed;
            Context = context;
        }

        public Key Key { get; private set; }
        public ModifierKeys? ModifierKeys { get; private set; }
        public Action WhenPressed { get; private set; }
        public IScreen Context { get; private set; } 

        public bool Matches(Key key)
        {
            if (key != Key) return false;
            if (ModifierKeys.HasValue && Keyboard.Modifiers != ModifierKeys) return false;

            WhenPressed();
            return true;
        }
    }
}
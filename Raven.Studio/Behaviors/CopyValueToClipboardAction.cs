using System.Windows;
using System.Windows.Interactivity;


namespace Raven.Studio.Behaviors
{
    public class CopyValueToClipboardAction : TriggerAction<FrameworkElement>
    {
        public static readonly DependencyProperty ValueProperty =
            DependencyProperty.Register("Value", typeof (object), typeof (CopyValueToClipboardAction), new PropertyMetadata(default(object)));

        public object Value
        {
            get { return (object) GetValue(ValueProperty); }
            set { SetValue(ValueProperty, value); }
        }

        protected override void Invoke(object parameter)
        {
            if (Value != null)
                Clipboard.SetText(Value.ToString());
        }
    }
}
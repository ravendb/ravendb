using System;
using System.Net;
using System.Windows;
using System.Windows.Automation.Peers;
using System.Windows.Automation.Provider;
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
    public class DefaultButtonBehavior : Behavior<DataGrid>
    {
        public static readonly DependencyProperty DefaultButtonProperty =
            DependencyProperty.Register("DefaultButton", typeof(Button), typeof(DefaultButtonBehavior), new PropertyMetadata(default(Button)));

        public Button DefaultButton
        {
            get { return (Button) GetValue(DefaultButtonProperty); }
            set { SetValue(DefaultButtonProperty, value); }
        }

        protected override void OnAttached()
        {
            base.OnAttached();

            AssociatedObject.KeyUp += HandleKeyUp;
        }

        private void HandleKeyUp(object sender, KeyEventArgs e)
        {
            if (FocusManager.GetFocusedElement() == AssociatedObject
                && e.Key == Key.Enter
                && DefaultButton != null)
            {
                var peer = new ButtonAutomationPeer(DefaultButton);
                    peer.SetFocus();
                    AssociatedObject.Dispatcher.BeginInvoke(delegate
                    {
                        var invokeProv =
                            peer.GetPattern(PatternInterface.Invoke) as IInvokeProvider;
                        if (invokeProv != null)
                            invokeProv.Invoke();
                    });
            }
        }

        protected override void OnDetaching()
        {
            base.OnDetaching();

            AssociatedObject.KeyUp -= HandleKeyUp;
        }
    }
}

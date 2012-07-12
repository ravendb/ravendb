using System;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using Microsoft.Expression.Interactivity.Core;
using Raven.Abstractions.Extensions;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Linq;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Controls
{
    public partial class BusynessIndicator : UserControl
    {
        public static readonly DependencyProperty BusyBodyProperty =
            DependencyProperty.Register("BusyBody", typeof(INotifyBusyness), typeof(BusynessIndicator), new PropertyMetadata(default(INotifyBusyness), HandleSourceChanged));
        private bool _isLoaded;

        private static void HandleSourceChanged(DependencyObject d, DependencyPropertyChangedEventArgs e)
        {
            var notifier = d as BusynessIndicator;

            if (e.OldValue != null)
            {
                notifier.StopListening(e.OldValue as INotifyBusyness);
            }

            if (e.NewValue != null && notifier._isLoaded)
            {
               notifier.StartListening();
            }
        }

        private void IsBusyChanged(object sender, EventArgs e)
        {
            if (Dispatcher.CheckAccess())
            {
                UpdateState();
            }
            else
            {
                Dispatcher.BeginInvoke(UpdateState);
            }
        }

        private void UpdateState()
        {
            if (BusyBody != null && BusyBody.IsBusy)
            {
                VisualStateManager.GoToState(this, "Busy", true);
            }
            else
            {
                VisualStateManager.GoToState(this, "Idle", true);
            }
        }

        public INotifyBusyness BusyBody
        {
            get { return (INotifyBusyness)GetValue(BusyBodyProperty); }
            set { SetValue(BusyBodyProperty, value); }
        }

        public BusynessIndicator()
        {
            InitializeComponent();

            VisualStateManager.GoToState(this, "Idle", true);

            Loaded += delegate
                          {
                              _isLoaded = true;
                              StartListening();
                              UpdateState();
                          };

            Unloaded += delegate
            {
                StopListening(BusyBody);
                _isLoaded = false;
            };
        }

        private void StartListening()
        {
            if (BusyBody != null)
            {
                BusyBody.IsBusyChanged += IsBusyChanged;
            }
        }

        private void StopListening(INotifyBusyness busyBody)
        {
            if (busyBody != null)
            {
                busyBody.IsBusyChanged -= IsBusyChanged;
            }
        }
    }
}

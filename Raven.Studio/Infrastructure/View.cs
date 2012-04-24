using System;
using System.Net;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;

namespace Raven.Studio.Infrastructure
{
    public class View : UserControl
    {
        public View()
        {
            Loaded += HandleLoaded;
            Unloaded += HandleUnloaded;
        }

        private void HandleLoaded(object sender, RoutedEventArgs e)
        {
            if (DataContext is ViewModel)
            {
                (DataContext as ViewModel).NotifyViewLoaded();
            }
        }

        private void HandleUnloaded(object sender, RoutedEventArgs e)
        {
            if (DataContext is ViewModel)
            {
                (DataContext as ViewModel).NotifyViewUnloaded();
            }
        }
    }
}

using System.Windows;
using System.Windows.Controls;

namespace Raven.Studio.Infrastructure
{
    public class View : UserControl
    {
        private bool isLoaded;

        public View()
        {
            Loaded += HandleLoaded;
            Unloaded += HandleUnloaded;
            DataContextChanged += HandleDataContextChanged;
        }

        private void HandleDataContextChanged(object sender, DependencyPropertyChangedEventArgs e)
        {
            if (isLoaded)
            {
                var oldViewModel = e.OldValue as ViewModel;
                if (oldViewModel != null)
                    oldViewModel.NotifyViewUnloaded();

                var newViewModel = e.NewValue as ViewModel;
                if (newViewModel != null)
                    newViewModel.NotifyViewLoaded();
            }
        }

        private void HandleLoaded(object sender, RoutedEventArgs e)
        {
            if (DataContext is ViewModel)
                (DataContext as ViewModel).NotifyViewLoaded();

            isLoaded = true;
        }

        private void HandleUnloaded(object sender, RoutedEventArgs e)
        {
            if (DataContext is ViewModel)
                (DataContext as ViewModel).NotifyViewUnloaded();

            isLoaded = false;
        }
    }
}
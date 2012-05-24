using System.Windows;
using System.Windows.Controls;

namespace Raven.Studio.Infrastructure
{
    public class DialogView : ChildWindow
    {
        public DialogView()
        {
            DataContextChanged += HandleDataContextChanged;
            Loaded += HandleLoaded;
            Unloaded += HandleUnloaded;
        }

        private void HandleUnloaded(object sender, RoutedEventArgs e)
        {
            if (HasModel)
            {
                Model.NotifyViewUnloaded();
            }
        }

        private void HandleLoaded(object sender, RoutedEventArgs e)
        {
            if (HasModel)
            {
                Model.NotifyViewLoaded();
            }
        }

        private void HandleDataContextChanged(object sender, DependencyPropertyChangedEventArgs e)
        {
            var oldModel = e.OldValue as DialogViewModel;
            if (oldModel != null)
            {
                oldModel.CloseRequested -= HandleCloseRequested;
            }

            var newModel = e.NewValue as DialogViewModel;
            if (newModel != null)
            {
                newModel.CloseRequested += HandleCloseRequested;
            }
        }

        private void HandleCloseRequested(object sender, CloseRequestedEventArgs e)
        {
            DialogResult = e.DialogResult;
        }

        public DialogViewModel Model
        {
            get { return DataContext as DialogViewModel; }
            set { DataContext = value; }
        }

        protected bool HasModel
        {
            get { return Model != null; }
        }
    }
}

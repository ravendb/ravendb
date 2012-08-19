using System;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using System.Windows.Interactivity;
using Raven.Abstractions;

namespace Raven.Studio.Behaviors
{
    public class DataGridRowDoubleClickCommandBehavior : Behavior<DataGrid>
    {
        private const double DoubleClickInterval = 300;

        public static readonly DependencyProperty CommandProperty =
            DependencyProperty.Register("Command", typeof (ICommand), typeof (DataGridRowDoubleClickCommandBehavior), new PropertyMetadata(default(ICommand)));

        public ICommand Command
        {
            get { return (ICommand) GetValue(CommandProperty); }
            set { SetValue(CommandProperty, value); }
        }

        private DataGridRow _lastClickedRow;
        private DateTime _lastClickTime;

        protected override void OnAttached()
        {
            base.OnAttached();

            AssociatedObject.LoadingRow += HandleLoadingRow;
            AssociatedObject.UnloadingRow += HandleUnloadingRow;
        }

        protected override void OnDetaching()
        {
            base.OnDetaching();

            AssociatedObject.LoadingRow -= HandleLoadingRow;
            AssociatedObject.UnloadingRow -= HandleUnloadingRow;
        }

        private void HandleUnloadingRow(object sender, DataGridRowEventArgs e)
        {
            e.Row.MouseLeftButtonUp -= HandleRowLeftButtonDown;
        }

        private void HandleLoadingRow(object sender, DataGridRowEventArgs e)
        {
            e.Row.MouseLeftButtonUp += HandleRowLeftButtonDown;
        }

        private void HandleRowLeftButtonDown(object sender, MouseButtonEventArgs e)
        {
			if (sender == _lastClickedRow && (SystemTime.UtcNow - _lastClickTime).TotalMilliseconds < DoubleClickInterval)
           {
               var data = _lastClickedRow.DataContext;
               if (Command != null && Command.CanExecute(data))
               {
                   Command.Execute(data);
               }
           }

			_lastClickTime = SystemTime.UtcNow;
            _lastClickedRow = sender as DataGridRow;
        }
    }
}

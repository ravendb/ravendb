using System;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using System.Windows.Interactivity;

namespace Raven.Studio.Behaviors
{
    public class DataGridRowDoubleClickTrigger: TriggerBase<DataGrid>
    {
        private const double DoubleClickInterval = 300;

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
           if (sender == _lastClickedRow && (DateTime.Now - _lastClickTime).TotalMilliseconds < DoubleClickInterval)
           {
               InvokeActions(null);
           }

            _lastClickTime = DateTime.Now;
            _lastClickedRow = sender as DataGridRow;
        }
    }
}

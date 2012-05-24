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
    public partial class DataFetchErrorNotifier : UserControl
    {
        public static readonly DependencyProperty ErrorSourceProperty =
            DependencyProperty.Register("ErrorSource", typeof (INotifyOnDataFetchErrors), typeof (DataFetchErrorNotifier), new PropertyMetadata(default(INotifyOnDataFetchErrors), HandleSourceChanged));

        private static readonly DependencyProperty ErrorTextProperty =
            DependencyProperty.Register("ErrorText", typeof (string), typeof (DataFetchErrorNotifier), new PropertyMetadata(default(string)));

        private ICommand retryCommand;
        private ICommand copyErrorToClipboard;
        private Exception firstException;

        private static void HandleSourceChanged(DependencyObject d, DependencyPropertyChangedEventArgs e)
        {
            var notifier = d as DataFetchErrorNotifier;

            if (e.OldValue != null)
            {
                (e.OldValue as INotifyOnDataFetchErrors).DataFetchError -= notifier.HandleError;
                (e.OldValue as INotifyOnDataFetchErrors).FetchCompleted -= notifier.HandleFetchCompleted;
                (e.OldValue as INotifyOnDataFetchErrors).FetchStarting -= notifier.HandleFetchStarting;
            }

            if (e.NewValue != null)
            {
                (e.NewValue as INotifyOnDataFetchErrors).DataFetchError += notifier.HandleError;
                (e.NewValue as INotifyOnDataFetchErrors).FetchCompleted += notifier.HandleFetchCompleted;
                (e.NewValue as INotifyOnDataFetchErrors).FetchStarting += notifier.HandleFetchStarting;
            }
        }

        private void HandleFetchStarting(object sender, EventArgs e)
        {
            ClearError();
            VisualStateManager.GoToState(this, "Busy", true);
        }

        private void HandleFetchCompleted(object sender, EventArgs e)
        {
            VisualStateManager.GoToState(this, "Idle", true);
        }

        private void ClearError()
        {
            if (firstException != null)
            {
                firstException = null;
                VisualStateManager.GoToState(this, "NoError", true);
            }
        }

        private void HandleError(object sender, DataFetchErrorEventArgs e)
        {
            if (firstException == null)
            {
                VisualStateManager.GoToState(this, "Error", true);
            }

            firstException = e.Error;

            if (firstException is AggregateException)
            {
                var errorText = (firstException as AggregateException).ExtractSingleInnerException().Message;
                errorText = TryExtractMessageFromJSONError(errorText);
                ErrorText = errorText;
            }
            else
            {
                ErrorText = firstException.Message;
            }
        }

        private string TryExtractMessageFromJSONError(string errorText)
        {
            try
            {
                var jObject = RavenJObject.Parse(errorText);
                return jObject["Message"] != null ? jObject["Message"].Value<string>() : errorText;
            }
            catch (Exception)
            {
                return errorText;
            }
        }

        public INotifyOnDataFetchErrors ErrorSource
        {
            get { return (INotifyOnDataFetchErrors) GetValue(ErrorSourceProperty); }
            set { SetValue(ErrorSourceProperty, value); }
        }

        public string ErrorText
        {
            get { return (string)GetValue(ErrorTextProperty); }
            private set { SetValue(ErrorTextProperty, value); }
        }

        public ICommand Retry { get { return retryCommand ?? (retryCommand = new ActionCommand(HandleRetry)); } }

        public ICommand CopyErrorToClipboard { get { return copyErrorToClipboard ?? (copyErrorToClipboard = new ActionCommand(HandleCopyToClipboard)); } }

        public DataFetchErrorNotifier()
        {
            InitializeComponent();

            VisualStateManager.GoToState(this, "NoError", true);
        }

        private void HandleRetry()
        {
            ErrorSource.Retry();
        }

        private void HandleCopyToClipboard()
        {
            if (firstException != null)
            {
                Clipboard.SetText(firstException.ToString());
            }
        }

    }
}

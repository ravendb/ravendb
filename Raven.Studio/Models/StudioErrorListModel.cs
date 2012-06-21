using System;
using System.ComponentModel;
using System.Net;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;
using Microsoft.Expression.Interactivity.Core;
using Raven.Studio.Infrastructure;
using Raven.Studio.Messages;
using System.Linq;

namespace Raven.Studio.Models
{
    public class StudioErrorListModel : DialogViewModel
    {
        private ICollectionView collectionView;
        private ICommand clear;
        private ICommand copyErrorDetailsToClipboard;
        private Notification selectedItem;
        private ICommand close;

        public ICollectionView Errors
        {
            get
            {

                return collectionView ?? (collectionView = new PagedCollectionView(ApplicationModel.Current.Notifications)
                                            {
                                                Filter = item => ((Notification) item).Level == NotificationLevel.Error
                                            });
            }
        }

        public Notification SelectedItem
        {
            get { return selectedItem; }
            private set
            {
                selectedItem = value;
                OnPropertyChanged(() => SelectedItem);
            }
        }

        public ICommand CloseCommand
        {
            get { return close ?? (close = new ActionCommand(() => Close(true))); }
        }

        public ICommand Clear
        {
            get { return clear ?? (clear = new ActionCommand(() => ApplicationModel.Current.Notifications.Clear())); }
        }

        public ICommand CopyErrorDetailsToClipboard
        {
            get
            {
                return copyErrorDetailsToClipboard ??
                       (copyErrorDetailsToClipboard = new ActionCommand(HandleCopyErrorDetailsToClipboard));
            }
        }

        private void HandleCopyErrorDetailsToClipboard(object parameter)
        {
            var notification = parameter as Notification;
            if (notification == null)
            {
                return;
            }

            Clipboard.SetText(notification.Details);
        }

        protected override void OnViewLoaded()
        {
            base.OnViewLoaded();

            SelectedItem = ApplicationModel.Current.Notifications.LastOrDefault(n => n.Level == NotificationLevel.Error);
        }
    }
}

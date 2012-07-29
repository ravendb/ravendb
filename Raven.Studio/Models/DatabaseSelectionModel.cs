using System;
using System.Collections.Specialized;
using System.ComponentModel;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;
using Raven.Studio.Commands;
using Raven.Studio.Infrastructure;
using Raven.Studio.Extensions;
using System.Reactive.Linq;

namespace Raven.Studio.Models
{
    public class DatabaseSelectionModel : ViewModel
    {
        private ChangeDatabaseCommand changeDatabase;

        public DatabaseSelectionModel()
        {
            changeDatabase = new ChangeDatabaseCommand();
        }

        public bool SingleTenant
        {
            get { return ApplicationModel.Current.Server.Value.SingleTenant; }
        }

        public BindableCollection<string> Databases
        {
            get { return ApplicationModel.Current.Server.Value.Databases; }
        }

        public string SelectedDatabase
        {
            get { return ApplicationModel.Database.Value != null ? ApplicationModel.Database.Value.Name : null; }
            set
            {
                if (changeDatabase.CanExecute(value))
                    changeDatabase.Execute(value);
            }
        }

        protected override void OnViewLoaded()
        {
            ApplicationModel.Current.Server.Value.SelectedDatabase
                .ObservePropertyChanged()
                .TakeUntil(Unloaded)
                .Subscribe(_ => OnPropertyChanged(() => SelectedDatabase));

            // when the database list changes we need to trigger a refresh of the SelectedDatabase command,
            // but we only want to do it after each batch of changes
            ApplicationModel.Current.Server.Value.Databases.ObserveCollectionChanged()
                .Throttle(TimeSpan.FromMilliseconds(10))
                .ObserveOnDispatcher()
                .TakeUntil(Unloaded)
                .Subscribe(_ => OnPropertyChanged(() => SelectedDatabase));

            OnPropertyChanged(() => SelectedDatabase);
        }

        protected override void OnViewUnloaded()
        {
        }
    }
}

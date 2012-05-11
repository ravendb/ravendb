using System;
using System.Net;
using System.Reactive;
using System.Reactive.Subjects;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;
using Raven.Studio.Models;

namespace Raven.Studio.Infrastructure
{
    public class ViewModel : Model
    {
        private Subject<Unit> unloadedSubject;

        public void NotifyViewLoaded()
        {
            IsLoaded = true;
            OnViewLoaded();
        }

        public void NotifyViewUnloaded()
        {
            if (unloadedSubject != null)
            {
                unloadedSubject.OnNext(Unit.Default);
            }

            OnViewUnloaded();

            IsLoaded = false;
        }

        protected virtual void OnViewUnloaded()
        {
            
        }

        protected virtual void OnViewLoaded()
        {
            
        }

        protected IObservable<Unit> Unloaded
        {
            get { return unloadedSubject ?? (unloadedSubject = new Subject<Unit>()); }
        }

        protected bool IsLoaded { get; private set; }
        protected PerDatabaseState PerDatabaseState
        {
            get { return ApplicationModel.Current.State.Databases[ApplicationModel.Database.Value]; }
        }
    }
}

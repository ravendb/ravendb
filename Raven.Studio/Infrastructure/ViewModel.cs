using System;
using System.Reactive;
using System.Reactive.Subjects;
using Raven.Studio.Commands;
using Raven.Studio.Models;

namespace Raven.Studio.Infrastructure
{
    public class ViewModel : Model
    {
        private Subject<Unit> unloadedSubject;

        public void NotifyViewLoaded()
        {
            if (!IsLoaded)
            {
                IsLoaded = true;
                OnViewLoaded();
            }
        }

        public void NotifyViewUnloaded()
        {
            if (IsLoaded)
            {
                if (unloadedSubject != null)
                {
                    unloadedSubject.OnNext(Unit.Default);
                }

                OnViewUnloaded();

                IsLoaded = false;
            }
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

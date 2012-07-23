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
			IsLoaded = true;
			OnViewLoaded();
		}

		public override System.Threading.Tasks.Task TimerTickedAsync()
		{
			if (ApplicationModel.Current.Server.Value.CreateNewDatabase)
			{
				ApplicationModel.Current.Server.Value.CreateNewDatabase = false;
				Command.ExecuteCommand(new CreateDatabaseCommand());
			}
			return base.TimerTickedAsync();
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

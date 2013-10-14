using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Windows.Input;
using Microsoft.Expression.Interactivity.Core;
using Raven.Client.Document;
using Raven.Json.Linq;
using Raven.Studio.Infrastructure;
using Raven.Abstractions.Data;
using System.Linq;
using Raven.Client.Connection;
using Raven.Studio.Messages;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Stats
{
	public class AlertsStatusSectionModel : StatusSectionModel
	{
		public ObservableCollection<AlertProxy> Alerts { get; set; }
		public ObservableCollection<Alert> ServerAlerts { get; set; }
		public ObservableCollection<AlertProxy> UnobservedAlerts { get; set; }
		public ObservableCollection<AlertProxy> AlertsToSee { get; set; }
		public Observable<bool> ShowObserved { get; set; }

		public AlertsStatusSectionModel()
		{
			SectionName = "Alerts";
			Alerts = new ObservableCollection<AlertProxy>();
			SelectedAlert = new Observable<AlertProxy>();
			UnobservedAlerts = new ObservableCollection<AlertProxy>();
			ServerAlerts = new ObservableCollection<Alert>();
			ShowObserved = new Observable<bool>();

			AlertsToSee = Alerts;
			RegisterToChanges();
			GetAlertsFromServer();
		}

		private void RegisterToChanges()
		{
			ShowObserved.PropertyChanged += (sender, args) =>
			{
				AlertsToSee = ShowObserved.Value ? Alerts : UnobservedAlerts;
				OnPropertyChanged(() => AlertsToSee);
			};

			ServerAlerts.CollectionChanged += (sender, args) =>
			{
				Alerts.Clear();
				foreach (var serverAlert in ServerAlerts)
				{
					Alerts.Add(new AlertProxy(serverAlert));
				}
			};

			ShowObserved.Value = false;

			Alerts.CollectionChanged += (sender, args) => UpdateUnobserved();

			OnPropertyChanged(() => ServerAlerts);
		}

		private void GetAlertsFromServer()
		{
			ApplicationModel
				.DatabaseCommands
				.GetAsync(Constants.RavenAlerts)
				.ContinueOnSuccessInTheUIThread(doc =>
				{
					if(doc == null)
					{
						ServerAlerts.Clear();
						return;
					}
					var alerts = doc.DataAsJson.Deserialize<AlertsDocument>(new DocumentConvention());
					ServerAlerts.Clear();

					foreach (var alert in alerts.Alerts)
					{
						ServerAlerts.Add(alert);
					}

				});
		}

		public void UpdateUnobserved()
		{
			UnobservedAlerts.Clear();
			foreach (var alert in Alerts.Where(alert => alert.Observed.Value == false))
			{
				UnobservedAlerts.Add(alert);
			}
		}

		public ICommand ToggleView { get { return new ToggleViewCommand(this); } }
		public ICommand CheckAll { get { return new MarkAllCommand(this, true); } }
		public ICommand UncheckAll { get { return new MarkAllCommand(this, false); } }
		public ICommand DeleteSelectedAlert { get { return new DeleteAlertCommand(this); } }
		public ICommand DeleteAllObserved { get { return new DeleteAllObservedCommand(this); } }
		public ICommand SaveChanges { get { return new SaveAlertsCommand(this); } }
		public ICommand Refresh{get{return new ActionCommand(GetAlertsFromServer);}}
		public Observable<AlertProxy> SelectedAlert { get; set; }
	}

	public class SaveAlertsCommand : Command
	{
		private readonly AlertsStatusSectionModel alertsStatusSectionModel;

		public SaveAlertsCommand(AlertsStatusSectionModel alertsStatusSectionModel)
		{
			this.alertsStatusSectionModel = alertsStatusSectionModel;
		}

		public override void Execute(object parameter)
		{
			var alertsDocument = new AlertsDocument
			{
				Alerts = new List<Alert>(alertsStatusSectionModel.ServerAlerts)
			};

			ApplicationModel.DatabaseCommands
				.PutAsync(Constants.RavenAlerts, null,RavenJObject.FromObject(alertsDocument),new RavenJObject())
				.ContinueOnSuccessInTheUIThread(() => ApplicationModel.Current.Notifications.Add(new Notification("Alerts Saved")));
		}
	}

	public class DeleteAllObservedCommand : Command
	{
		private readonly AlertsStatusSectionModel alertsStatusSectionModel;

		public DeleteAllObservedCommand(AlertsStatusSectionModel alertsStatusSectionModel)
		{
			this.alertsStatusSectionModel = alertsStatusSectionModel;
		}

		public override void Execute(object parameter)
		{
			var deleteList = alertsStatusSectionModel.AlertsToSee.Where(proxy => proxy.Observed.Value).ToList();

			foreach (var alertProxy in deleteList)
			{
				alertsStatusSectionModel.ServerAlerts.Remove(alertProxy.Alert);
			}	
		}
	}

	public class DeleteAlertCommand : Command
	{
		private readonly AlertsStatusSectionModel alertsStatusSectionModel;

		public DeleteAlertCommand(AlertsStatusSectionModel alertsStatusSectionModel)
		{
			this.alertsStatusSectionModel = alertsStatusSectionModel;
		}

		public override bool CanExecute(object parameter)
		{
			return parameter is AlertProxy;
		}

		public override void Execute(object parameter)
		{
			var alert = parameter as AlertProxy;

			alertsStatusSectionModel.ServerAlerts.Remove(alert.Alert);
		}
	}

	public class MarkAllCommand : Command
	{
		private readonly AlertsStatusSectionModel alertsStatusSectionModel;
		private readonly bool markAs;

		public MarkAllCommand(AlertsStatusSectionModel alertsStatusSectionModel, bool markAs)
		{
			this.alertsStatusSectionModel = alertsStatusSectionModel;
			this.markAs = markAs;
		}

		public override void Execute(object parameter)
		{
			foreach (var alert in alertsStatusSectionModel.AlertsToSee)
			{
				alert.Observed.Value = markAs;
			}
		}
	}

	public class ToggleViewCommand : Command
	{
		private readonly AlertsStatusSectionModel alertsStatusSectionModel;

		public ToggleViewCommand(AlertsStatusSectionModel alertsStatusSectionModel)
		{
			this.alertsStatusSectionModel = alertsStatusSectionModel;
		}

		public override void Execute(object parameter)
		{
			alertsStatusSectionModel.ShowObserved.Value = !alertsStatusSectionModel.ShowObserved.Value;
			alertsStatusSectionModel.UpdateUnobserved();
		}
	}
}
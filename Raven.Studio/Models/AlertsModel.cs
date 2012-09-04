using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Windows.Input;
using Raven.Studio.Features.Alerts;
using Raven.Studio.Infrastructure;
using Raven.Abstractions.Data;
using System.Linq;

namespace Raven.Studio.Models
{
	public class AlertsModel : ViewModel
	{
		public ObservableCollection<AlertProxy> Alerts { get; set; }
		public ObservableCollection<Alert> ServerAlerts { get; set; }
		public ObservableCollection<AlertProxy> UnobservedAlerts { get; set; }
		public ObservableCollection<AlertProxy> AlertsToSee { get; set; }
		public Observable<bool> ShowObserved { get; set; }

		public AlertsModel()
		{
			Alerts = new ObservableCollection<AlertProxy>();
			UnobservedAlerts = new ObservableCollection<AlertProxy>();
			ServerAlerts = new ObservableCollection<Alert>();
			ShowObserved = new Observable<bool>();

			AlertsToSee = Alerts;

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

			//TODO: Query for data and delete sample data

			ServerAlerts.Add(new Alert()
			{
				AlertLevel = AlertLevel.Warning,
				Title = "Title",
				Database = "NO Database",
				Message = "This warning has not been seen",
				Observed = false
			});

			ServerAlerts.Add(new Alert()
			{
				AlertLevel = AlertLevel.Error,
				Message = "This error was seen",
				Observed = true
			});

			OnPropertyChanged(() => Alerts);
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
	}

	public class DeleteAllObservedCommand : Command
	{
		private readonly AlertsModel alertsModel;

		public DeleteAllObservedCommand(AlertsModel alertsModel)
		{
			this.alertsModel = alertsModel;
		}

		public override void Execute(object parameter)
		{
			var deleteList = alertsModel.AlertsToSee.Where(proxy => proxy.Observed.Value).ToList();

			foreach (var alertProxy in deleteList)
			{
				alertsModel.ServerAlerts.Remove(alertProxy.Alert);
			}	
		}
	}

	public class DeleteAlertCommand : Command
	{
		private readonly AlertsModel alertsModel;

		public DeleteAlertCommand(AlertsModel alertsModel)
		{
			this.alertsModel = alertsModel;
		}

		public override bool CanExecute(object parameter)
		{
			return parameter is AlertProxy;
		}

		public override void Execute(object parameter)
		{
			var alert = parameter as AlertProxy;

			alertsModel.ServerAlerts.Remove(alert.Alert);
		}
	}

	public class MarkAllCommand : Command
	{
		private readonly AlertsModel alertsModel;
		private readonly bool markAs;

		public MarkAllCommand(AlertsModel alertsModel, bool markAs)
		{
			this.alertsModel = alertsModel;
			this.markAs = markAs;
		}

		public override void Execute(object parameter)
		{
			foreach (var alert in alertsModel.AlertsToSee)
			{
				alert.Observed.Value = markAs;
			}
		}
	}

	public class ToggleViewCommand : Command
	{
		private readonly AlertsModel alertsModel;

		public ToggleViewCommand(AlertsModel alertsModel)
		{
			this.alertsModel = alertsModel;
		}

		public override void Execute(object parameter)
		{
			alertsModel.ShowObserved.Value = !alertsModel.ShowObserved.Value;
			alertsModel.UpdateUnobserved();
		}
	}
}
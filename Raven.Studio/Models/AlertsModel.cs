using System;
using System.Collections.ObjectModel;
using Raven.Studio.Infrastructure;
using Raven.Abstractions.Data;

namespace Raven.Studio.Models
{
	public class AlertsModel : ViewModel
	{
		public ObservableCollection<Alert> Alerts { get; set;}

		public AlertsModel()
		{
			Alerts = new ObservableCollection<Alert>();

			//TODO: Query for data and delete sample data

			Alerts.Add(new Alert
			{
				AlertLevel = AlertLevel.Warning,
				Title = "Title",
				Database = "NO Database",
				Message = "This warning has not been seen",
				Observed = false
			});

			Alerts.Add(new Alert
			{
				AlertLevel = AlertLevel.Error,
				Message = "This error was seen",
				Observed = true
			});

			OnPropertyChanged(() => Alerts);
		}
	}
}
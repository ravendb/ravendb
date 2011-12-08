// -----------------------------------------------------------------------
//  <copyright file="LogsModel.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Net;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Studio.Infrastructure;
using Raven.Studio.Messages;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Logs
{
	public class LogsModel : ViewModel
	{
		private bool stopTicking;

		public BindableCollection<LogItem> Logs { get; private set; }

		public LogsModel()
		{
			ModelUrl = "/logs";
			Logs = new BindableCollection<LogItem>(log => log.TimeStamp, new KeysComparer<LogItem>(x => x.Message));
		}

		protected override Task LoadedTimerTickedAsync()
		{
			if (stopTicking)
				return null;

			return DatabaseCommands.GetLogsAsync(showErrorsOnly)
				.ContinueOnSuccess(logs => Logs.Match(logs))
				.CatchIgnore<WebException>(() =>
				                           	{
				                           		ApplicationModel.Current.AddNotification(new Notification("Logs end point is not enabled.", NotificationLevel.Info));
				                           		stopTicking = true;
				                           	});
		}

		private bool showErrorsOnly;
		public bool ShowErrorsOnly
		{
			get { return showErrorsOnly; }
			set
			{
				showErrorsOnly = value;
				OnPropertyChanged();
			}
		}

		public override void LoadModelParameters(string parameters)
		{
			ShowErrorsOnly = new UrlParser(parameters).Path.Trim('/') == "error";
		}
	}
}
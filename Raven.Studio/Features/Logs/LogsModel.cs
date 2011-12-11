// -----------------------------------------------------------------------
//  <copyright file="LogsModel.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
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
		public BindableCollection<LogItem> Logs { get; private set; }

		public LogsModel()
		{
			ModelUrl = "/logs";
			Logs = new BindableCollection<LogItem>(log => log.TimeStamp, new KeysComparer<LogItem>(x => x.Message));
		}

		protected override Task LoadedTimerTickedAsync()
		{
			if (IsLogsEnabled == false)
				return null;

			return DatabaseCommands.GetLogsAsync(showErrorsOnly)
				.ContinueOnSuccess(logs => Logs.Match(logs))
				.CatchIgnore<WebException>(() =>
				                           	{
				                           		ApplicationModel.Current.AddNotification(new Notification("Logs end point is not enabled.", NotificationLevel.Info));
				                           		LogsIsNotEnabled();
				                           	});
		}
		
		private void LogsIsNotEnabled()
		{
			IsLogsEnabled = false;
			EnablingLogsInstructions = GetTextFromResource("Raven.Studio.Features.Logs.DefaultLogging.config");
		}

		private static string GetTextFromResource(string name)
		{
			using (var stream = typeof (LogsModel).Assembly.GetManifestResourceStream(name))
			{
				if (stream == null)
					throw new InvalidOperationException("Could not find the following resource: " + name);
				return new StreamReader(stream).ReadToEnd();
			}
		}

		private bool? isLogsEnabled;
		public bool? IsLogsEnabled
		{
			get { return isLogsEnabled; }
			set
			{
				isLogsEnabled = value;
				OnPropertyChanged();
			}
		}

		private string enablingLogsInstructions;
		public string EnablingLogsInstructions
		{
			get { return enablingLogsInstructions; }
			set
			{
				enablingLogsInstructions = value;
				OnPropertyChanged();
			}
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
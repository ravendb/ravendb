// -----------------------------------------------------------------------
//  <copyright file="LogsModel.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using System.Net;
using System.Threading.Tasks;
using System.Windows.Input;
using Raven.Abstractions.Data;
using Raven.Studio.Commands;
using Raven.Studio.Infrastructure;
using System.Linq;

namespace Raven.Studio.Features.Logs
{
	public class LogsModel : PageViewModel
	{
		public BindableCollection<LogItem> Logs { get; private set; }
		public BindableCollection<LogItem> DisplayedLogs { get; private set; }

		public LogsModel()
		{
			ModelUrl = "/logs";
			Logs = new BindableCollection<LogItem>(log => log.TimeStamp, new KeysComparer<LogItem>(x => x.Message));
			DisplayedLogs = new BindableCollection<LogItem>(log => log.TimeStamp, new KeysComparer<LogItem>(x => x.Message));
			Logs.CollectionChanged += (sender, args) => OnPropertyChanged(() => PendingLogs);
			DisplayedLogs.CollectionChanged += (sender, args) => OnPropertyChanged(() => PendingLogs);
		}

		protected override Task LoadedTimerTickedAsync()
		{
			if (IsLogsEnabled == false || Database.Value == null)
				return null;

			return DatabaseCommands.GetLogsAsync(showErrorsOnly)
				.ContinueOnSuccess(logs =>
				                   	{
										Logs.Match(logs.OrderByDescending(x=>x.TimeStamp).ToList(), () =>
										{
											if (DisplayedLogs.Count == 0)
												DisplayedLogs.Match(Logs);
										});
				                   		IsLogsEnabled = true;
				                   	})
				.CatchIgnore<WebException>(LogsIsNotEnabled);
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
				OnPropertyChanged(() => IsLogsEnabled);
			}
		}

		private string enablingLogsInstructions;
		public string EnablingLogsInstructions
		{
			get { return enablingLogsInstructions; }
			set
			{
				enablingLogsInstructions = value;
				OnPropertyChanged(() => EnablingLogsInstructions);
			}
		}

		private bool showErrorsOnly;
		public bool ShowErrorsOnly
		{
			get { return showErrorsOnly; }
			set
			{
				showErrorsOnly = value;
				OnPropertyChanged(() => ShowErrorsOnly);
			}
		}

		public int PendingLogs
		{
			get { return Logs.Count - DisplayedLogs.Count; }
		}

		public override void LoadModelParameters(string parameters)
		{
			ShowErrorsOnly = new UrlParser(parameters).Path.Trim('/') == "error";
		}

		public ICommand Refresh
		{
			get { return new ChangeFieldValueCommand<LogsModel>(this, x => DisplayedLogs.Match(Logs)); }
		}
	}
}

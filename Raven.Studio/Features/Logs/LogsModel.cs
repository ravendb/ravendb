// -----------------------------------------------------------------------
//  <copyright file="LogsModel.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client.Connection.Async;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Features.Logs
{
	public class LogsModel : Model
	{
		private readonly IAsyncDatabaseCommands databaseCommands;
		private bool showErrorsOnly;
		public BindableCollection<LogItem> Logs { get; private set; }

		public LogsModel(IAsyncDatabaseCommands databaseCommands, bool showErrorsOnly)
		{
			this.databaseCommands = databaseCommands;
			ShowErrorsOnly = showErrorsOnly;
			Logs = new BindableCollection<LogItem>(new PrimaryKeyComparer<LogItem>(log => log.TimeStamp));
		}

		protected override Task TimerTickedAsync()
		{
			return databaseCommands.GetLogsAsync(showErrorsOnly)
				.ContinueOnSuccess(logs => Logs.Match(logs));
		}

		public bool ShowErrorsOnly
		{
			get { return showErrorsOnly; }
			set
			{
				showErrorsOnly = value;
				OnPropertyChanged();
			}
		}
	}
}
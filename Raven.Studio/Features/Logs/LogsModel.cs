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
		private readonly bool showErrorsOnly;
		public BindableCollection<LogItem> Logs { get; private set; }
		public BindableCollection<LogItem> ErrorsLogs { get; private set; }

		public LogsModel(IAsyncDatabaseCommands databaseCommands, bool showErrorsOnly)
		{
			this.databaseCommands = databaseCommands;
			this.showErrorsOnly = showErrorsOnly;
			Logs = new BindableCollection<LogItem>(new PrimaryKeyComparer<LogItem>(log => log.Timestamp));
			ErrorsLogs = new BindableCollection<LogItem>(new PrimaryKeyComparer<LogItem>(log => log.Timestamp));
		}

		protected override Task TimerTickedAsync()
		{
			return databaseCommands.GetLogsAsync(showErrorsOnly)
				.ContinueOnSuccess(logs => Logs.Match(logs))
				.ContinueOnSuccess(() => databaseCommands.GetLogsAsync(true).ContinueOnSuccess(logs => ErrorsLogs.Match(logs)));
		}
	}
}
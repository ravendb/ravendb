// -----------------------------------------------------------------------
//  <copyright file="LogsModel.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Features.Logs
{
	public class LogsModel : ViewModel
	{
		public BindableCollection<LogItem> Logs { get; private set; }

		public LogsModel()
		{
			ModelUrl = "/logs";
			Logs = new BindableCollection<LogItem>(new PrimaryKeyComparer<LogItem>(log => log.TimeStamp));
		}

		protected override Task TimerTickedAsync()
		{
			return DatabaseCommands.GetLogsAsync(showErrorsOnly)
				.ContinueOnSuccess(logs => Logs.Match(logs));
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
			ShowErrorsOnly = GetParamAfter("/", parameters) == "error";
			ForceTimerTicked();
		}
	}
}
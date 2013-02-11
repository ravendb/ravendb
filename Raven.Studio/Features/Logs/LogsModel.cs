// -----------------------------------------------------------------------
//  <copyright file="LogsModel.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Threading.Tasks;
using System.Windows.Input;
using Raven.Abstractions.Data;
using Raven.Studio.Commands;
using Raven.Studio.Infrastructure;
using System.Linq;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Logs
{
	public class LogsModel : PageViewModel
	{
		public BindableCollection<LogItem> Logs { get; private set; }
		public BindableCollection<LogItem> DisplayedLogs { get; private set; }

		public LogsModel()
		{
			ModelUrl = "/logs";
			ApplicationModel.Current.Server.Value.RawUrl = null;

			Logs = new BindableCollection<LogItem>(log => log.TimeStamp, new KeysComparer<LogItem>(x => x.Message));
			DisplayedLogs = new BindableCollection<LogItem>(log => log.TimeStamp, new KeysComparer<LogItem>(x => x.Message));
			Logs.CollectionChanged += (sender, args) => OnPropertyChanged(() => PendingLogs);
			DisplayedLogs.CollectionChanged += (sender, args) => OnPropertyChanged(() => PendingLogs);
		}

		protected override Task LoadedTimerTickedAsync()
		{
			if (Database.Value == null)
				return null;

			return DatabaseCommands.GetLogsAsync(showErrorsOnly)
				.ContinueOnSuccess(logs => Logs.Match(logs.OrderByDescending(x => x.TimeStamp).ToList(), () =>
				{
					if (DisplayedLogs.Count == 0)
						DisplayedLogs.Match(Logs);
				}));
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
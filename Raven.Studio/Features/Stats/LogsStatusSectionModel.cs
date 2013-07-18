// -----------------------------------------------------------------------
//  <copyright file="LogsStatusSectionModel.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Reactive.Linq;
using System.Threading.Tasks;
using System.Windows.Input;
using Microsoft.Expression.Interactivity.Core;
using Raven.Abstractions.Data;
using Raven.Studio.Extensions;
using Raven.Studio.Infrastructure;
using System.Linq;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Stats
{
	public class LogsStatusSectionModel : StatusSectionModel
	{
		public BindableCollection<LogItem> Logs { get; private set; }
		public BindableCollection<LogItem> DisplayedLogs { get; private set; }

		public LogsStatusSectionModel()
		{
			SectionName = "Logs";
			ApplicationModel.Current.Server.Value.RawUrl = null;

			Logs = new BindableCollection<LogItem>(log => log.TimeStamp, new KeysComparer<LogItem>(x => x.Message));
			DisplayedLogs = new BindableCollection<LogItem>(log => log.TimeStamp, new KeysComparer<LogItem>(x => x.Message));
			Logs.CollectionChanged += (sender, args) => OnPropertyChanged(() => PendingLogs);
			DisplayedLogs.CollectionChanged += (sender, args) =>
			{
				OnPropertyChanged(() => PendingLogs);
				OnPropertyChanged(() => DisplayedLogs);
			};
		}

		public string SearchValue
		{
			get { return searchValue; }
			set
			{
				searchValue = value;
				OnPropertyChanged(() => SearchValue);
			}
		}

		public ICommand Search
		{
			get
			{
				return new ActionCommand(() =>
				{
					DisplayedLogs.Clear();

					if (string.IsNullOrWhiteSpace(SearchValue))
					{
						DisplayedLogs.Match(Logs);
						return;
					}

					var results = Logs.Where(item => item.Message.Contains(SearchValue, StringComparison.InvariantCultureIgnoreCase) 
						|| item.Exception.Contains(SearchValue, StringComparison.InvariantCultureIgnoreCase)).ToList();
					DisplayedLogs.Match(results);
				});
			}
		}

		protected override Task LoadedTimerTickedAsync()
		{
			return Database.Value == null ? null : ReloadLogs();
		}

		private bool showErrorsOnly;
		private string searchValue;
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
			get
			{
				return new ActionCommand(_ =>
				{
					SearchValue = "";
					DisplayLatestLogs();
				});
			}
		}
		public ICommand ErrorsOnly
		{
			get
			{
				return new ActionCommand(() =>
				{
					ShowErrorsOnly = true;
					ReloadLogs();
				});
			}
		}
		public ICommand ShowAll
		{
			get
			{
				return new ActionCommand(() =>
				{
					ShowErrorsOnly = false;
					ReloadLogs();
				});
			}
		}

		private Task ReloadLogs()
        {
            return DatabaseCommands.GetLogsAsync(showErrorsOnly)
                .ContinueOnSuccess(logs => Logs.Match(logs.OrderByDescending(x => x.TimeStamp).ToList(), () =>
                {
                    if (DisplayedLogs.Count == 0)
                        DisplayLatestLogs();
					OnPropertyChanged(() => DisplayedLogs);
                }));
        }

        private void DisplayLatestLogs()
        {
			if(string.IsNullOrWhiteSpace(SearchValue))
				DisplayedLogs.Match(Logs);
        }

	    protected override void OnViewLoaded()
	    {
	        ApplicationModel.Database
	                        .ObservePropertyChanged()
	                        .TakeUntil(Unloaded)
	                        .Subscribe(_ => ReloadLogs().ContinueOnSuccessInTheUIThread(DisplayLatestLogs).Catch());
	    }
	}
}
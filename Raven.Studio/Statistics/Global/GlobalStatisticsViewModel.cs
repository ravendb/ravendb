namespace Raven.Studio.Statistics.Global
{
	using Caliburn.Micro;
	using Plugin;
	using Raven.Database.Data;

	public class GlobalStatisticsViewModel : Screen, IRavenScreen
	{
		bool isBusy;
		DatabaseStatistics statistics;

		public GlobalStatisticsViewModel(IDatabase database)
		{
			DisplayName = "Global Statistics";
			Database = database;
		}

		public IDatabase Database { get; set; }

		public bool IsBusy
		{
			get { return isBusy; }
			set
			{
				isBusy = value;
				NotifyOfPropertyChange(() => IsBusy);
			}
		}

		public DatabaseStatistics Statistics
		{
			get { return statistics; }
			set
			{
				statistics = value;
				NotifyOfPropertyChange(() => Statistics);
			}
		}

		public SectionType Section
		{
			get { return SectionType.Statistics; }
		}

		protected override void OnActivate()
		{
			RefreshStatistics();
		}

		public void RefreshStatistics()
		{
			IsBusy = true;
			Database.Session.Advanced
				.AsyncDatabaseCommands
				.GetStatisticsAsync()
				.ContinueWith(x =>
				              	{
				              		Statistics = x.Result;
				              		IsBusy = false;
				              	});
		}
	}
}
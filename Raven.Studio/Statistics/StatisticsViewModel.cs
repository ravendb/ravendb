namespace Raven.Studio.Statistics
{
	using Caliburn.Micro;
	using Framework;
	using Plugin;
	using Raven.Database.Data;

	public class StatisticsViewModel : Screen, IRavenScreen
	{
		readonly IServer server;
		bool isBusy;
		DatabaseStatistics statistics;

		public StatisticsViewModel(IServer server)
		{
			DisplayName = "Statistics";
			this.server = server;
		}

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

			using (var session = server.OpenSession())
			{
				session.Advanced.AsyncDatabaseCommands
					.GetStatisticsAsync()
					.ContinueOnSuccess(x =>
					                   	{
					                   		Statistics = x.Result;
					                   		IsBusy = false;
					                   	});
			}
		}
	}
}
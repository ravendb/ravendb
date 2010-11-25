namespace Raven.ManagementStudio.UI.Silverlight.Plugins.Statistics.Global
{
    using Caliburn.Micro;
    using Raven.Database.Data;
    using Raven.ManagementStudio.Plugin;

    public class GlobalStatisticsScreenViewModel : Screen, IRavenScreen
    {
        private bool isBusy;
        private DatabaseStatistics statistics;

        public GlobalStatisticsScreenViewModel(IDatabase database)
        {
            this.DisplayName = "Global Statistics";
            this.Database = database;
        }

        public IDatabase Database { get; set; }

        public bool IsBusy
        {
            get { return this.isBusy; }
            set
            {
                this.isBusy = value;
                this.NotifyOfPropertyChange(() => this.IsBusy);
            }
        }

        public DatabaseStatistics Statistics
        {
            get
            {
                if (this.statistics == null)
                {
                    this.IsBusy = true;
                    this.Database.StatisticsSession.Load((result) =>
                    {
                        this.Statistics = result.Data;
                        this.IsBusy = false;
                    });
                }

                return this.statistics;
            }
            set
            {
                this.statistics = value;
                this.NotifyOfPropertyChange(() => this.Statistics);
            }
        }

        public void RefreshStatistics()
        {
            this.IsBusy = true;
            this.Database.StatisticsSession.Load((result) =>
            {
                this.Statistics = result.Data;
                this.IsBusy = false;
            });
        }

        #region IRavenScreen Members

        public IRavenScreen ParentRavenScreen { get; set; }

        #endregion
    }
}
namespace Raven.ManagementStudio.UI.Silverlight.Plugins.Statistics.Global
{
    using System.ComponentModel.Composition;
    using Caliburn.Micro;
    using Raven.ManagementStudio.Plugin;
    using Raven.ManagementStudio.UI.Silverlight.Messages;
    using Raven.ManagementStudio.UI.Silverlight.Plugins.Documents.Browse;

    [Export(typeof(IPlugin))]
    public class GlobalStatistics : IPlugin
    {
        #region IPlugin Members

        [Import]
        public IEventAggregator EventAggregator { get; set; }

        public string Name
        {
            get { return "Global"; }
        }

        public SectionType Section
        {
            get { return SectionType.Statistics; }
        }

        public IRavenScreen RelatedScreen
        {
            get
            {
                return new GlobalStatisticsScreenViewModel(this.Database);
            }
        }

        public IDatabase Database { get; set; }

        public object MenuView
        {
            get { return new GlobalStatisticsMenuIcon(); }
        }

        public void GoToScreen()
        {
            this.EventAggregator.Publish(new OpenNewScreen(this.RelatedScreen));
        }

        #endregion
    }
}
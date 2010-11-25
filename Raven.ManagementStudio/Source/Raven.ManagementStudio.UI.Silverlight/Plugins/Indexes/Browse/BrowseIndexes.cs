namespace Raven.ManagementStudio.UI.Silverlight.Plugins.Indexes.Browse
{
    using System.ComponentModel.Composition;
    using Caliburn.Micro;
    using Raven.ManagementStudio.Plugin;
    using Raven.ManagementStudio.UI.Silverlight.Messages;

    [Export(typeof(IPlugin))]
    public class BrowseIndexes : IPlugin
    {
        #region IPlugin Members

        [Import]
        public IEventAggregator EventAggregator { get; set; }

        public string Name
        {
            get { return "Browse"; }
        }

        public SectionType Section
        {
            get { return SectionType.Indexes; }
        }

        public IRavenScreen RelatedScreen
        {
            get
            {
                return new BrowseIndexesScreenViewModel(this.Database);
            }
        }

        public IDatabase Database { get; set; }

        public object MenuView
        {
            get { return new BrowseIndexesMenuIcon(); }
        }

        public void GoToScreen()
        {
            this.EventAggregator.Publish(new OpenNewScreen(this.RelatedScreen));
        }

        #endregion
    }
}
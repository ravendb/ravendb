namespace Raven.ManagementStudio.UI.Silverlight.Plugins.Documents.Browse
{
    using System.ComponentModel.Composition;
    using Caliburn.Micro;
    using Messages;
    using Plugin;

    [Export(typeof(IPlugin))]
    public class BrowseDocuments : IPlugin
    {
        [Import]
        public IEventAggregator EventAggregator { get; set; }

        public string Name
        {
            get { return "Browse"; }
        }

        public ISection Section
        {
            get { return null; }
        }

        public IRavenScreen RelatedScreen
        {
            get
            {
                return new DocumentsScreenViewModel(this.Database);
            }
        }

        public IDatabase Database { get; set; }

        public object MenuView
        {
            get { return new BrowseMenuIcon(); }
        }

        public void GoToScreen()
        {
            this.EventAggregator.Publish(new OpenNewScreen(this.RelatedScreen));
        }
    }
}
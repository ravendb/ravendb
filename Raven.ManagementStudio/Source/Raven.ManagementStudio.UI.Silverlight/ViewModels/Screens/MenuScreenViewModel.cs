namespace Raven.ManagementStudio.UI.Silverlight.ViewModels.Screens
{
    using System.Collections.Generic;
    using System.ComponentModel.Composition;
    using System.Linq;
    using Caliburn.Micro;
    using Plugin;

    public class MenuScreenViewModel : Screen, IRavenScreen
    {
        private IEnumerable<IPlugin> plugins;
        private IEnumerable<IPlugin> documentPlugins;
        private IEnumerable<IPlugin> collectionPlugins;
        private IEnumerable<IPlugin> indexPlugins;
        private IEnumerable<IPlugin> statisticPlugins;
        private IEnumerable<IPlugin> administrationPlugins;
        private IEnumerable<IPlugin> otherPlugins;
        private bool isBusy;

        public MenuScreenViewModel(IDatabase database)
        {
            this.Database = database;
            this.DisplayName = "Home";
            CompositionInitializer.SatisfyImports(this);
        }

        public bool IsBusy
        {
            get { return this.isBusy; }
            set
            {
                this.isBusy = value;
                NotifyOfPropertyChange(() => this.IsBusy);
            }
        }

        [Import]
        public IEventAggregator EventAggregator { get; set; }

        [ImportMany(AllowRecomposition = true)]
        public IEnumerable<IPlugin> Plugins
        {
            get
            {
                return this.plugins;
            }

            set
            {
                this.plugins = value;

                foreach (var plugin in this.plugins)
                {
                    plugin.Database = this.Database;
                }

                NotifyOfPropertyChange(() => this.Plugins);
                NotifyOfPropertyChange(() => this.DocumentPlugins);
                NotifyOfPropertyChange(() => this.CollectionPlugins);
                NotifyOfPropertyChange(() => this.IndexPlugins);
                NotifyOfPropertyChange(() => this.StatisticPlugins);
                NotifyOfPropertyChange(() => this.AdministrationPlugins);
                NotifyOfPropertyChange(() => this.OtherPlugins);
            }
        }

        public IEnumerable<IPlugin> DocumentPlugins
        {
            get { return documentPlugins ?? (documentPlugins = Plugins.Where(x => x.Section == SectionType.Documents)); }
        }

        public IEnumerable<IPlugin> CollectionPlugins
        {
            get { return collectionPlugins ?? (collectionPlugins = Plugins.Where(x => x.Section == SectionType.Collections)); }
        }

        public IEnumerable<IPlugin> IndexPlugins
        {
            get { return indexPlugins ?? (indexPlugins = Plugins.Where(x => x.Section == SectionType.Indexes)); }
        }

        public IEnumerable<IPlugin> StatisticPlugins
        {
            get { return statisticPlugins ?? (statisticPlugins = Plugins.Where(x => x.Section == SectionType.Statistics)); }
        }

        public IEnumerable<IPlugin> AdministrationPlugins
        {
            get { return administrationPlugins ?? (administrationPlugins = Plugins.Where(x => x.Section == SectionType.Administration)); }
        }

        public IEnumerable<IPlugin> OtherPlugins
        {
            get { return otherPlugins ?? (otherPlugins = Plugins.Where(x => x.Section == SectionType.Other)); }
        }

        public IDatabase Database { get; set; }

        public IRavenScreen ParentRavenScreen { get; set; }
    }
}
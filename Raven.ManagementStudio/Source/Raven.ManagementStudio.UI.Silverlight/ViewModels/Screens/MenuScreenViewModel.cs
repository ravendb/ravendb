namespace Raven.ManagementStudio.UI.Silverlight.ViewModels.Screens
{
    using System.Collections.Generic;
    using System.ComponentModel.Composition;
    using Caliburn.Micro;
    using Plugin;

    public class MenuScreenViewModel : Screen, IRavenScreen
    {
        private IEnumerable<IPlugin> plugins;

        public MenuScreenViewModel(IDatabase database)
        {
            this.Database = database;
            this.DisplayName = "Menu";
            CompositionInitializer.SatisfyImports(this);
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
            }
        }

        public IDatabase Database { get; set; }
    }
}